import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterator

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from common.schema import Record, make_event_id, now_utc_iso, to_utc_iso
from common.sqs import publish

URI = "wss://www.seismicportal.eu/standing_order/websocket"
PING_INTERVAL = 5

def parse(message: dict, ingestion_time: str) -> Record:
    """Convert one EMSC message to the shared record format."""
    props = message["data"]["properties"]
    return Record(
        event_id=make_event_id("seismic", props["unid"], props["lastupdate"]),
        source="seismic",
        event_time=to_utc_iso(props["time"]),
        ingestion_time=ingestion_time,
        location={"lat": props["lat"], "lon": props["lon"], "depth": props["depth"]},
        payload=message,
    )


async def stream_events() -> AsyncIterator[dict]:
    """get every message from the EMSC feed as the full parsed JSON dict.
    reconnects automatically (with backoff) if the connection drops.
    """
    async for ws in connect(URI, ping_interval=PING_INTERVAL):
        logging.info("Connected to %s", URI)
        try:
            async for message in ws:
                try:
                    yield json.loads(message)
                except json.JSONDecodeError:
                    logging.exception("Unable to parse json message: %r", message)
        except ConnectionClosed:
            logging.warning("Connection closed, reconnecting...")


async def main() -> None:
    async for message in stream_events():
        try:
            record = parse(message, now_utc_iso())
        except (KeyError, TypeError, ValueError):
            logging.exception(
                "Skipping message that does not fit the Record envelope: %r", message
            )
            continue
        # boto3 is blocking; run it in a thread so websocket pings aren't delayed.
        await asyncio.to_thread(publish, [record])
        logging.info("Sent %s %s", message.get("action"), record.event_id)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Close WebSocket")
