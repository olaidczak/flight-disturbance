import asyncio
import json
import logging
import os
import sys
from collections.abc import AsyncIterator

import boto3
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from common.schema import Record, make_event_id, now_utc_iso, to_utc_iso

URI = "wss://www.seismicportal.eu/standing_order/websocket"
PING_INTERVAL = 5

TOPIC_ARN = os.environ.get("TOPIC_ARN")
QUEUE_URL = os.environ.get("QUEUE_URL")

sns = boto3.client("sns")
sqs = boto3.client("sqs")


def parse(event: dict, ingestion_time: str | None = None) -> Record:
    """Convert one EMSC event to the shared record format."""
    data = event["data"]
    properties = data["properties"]
    event_time = to_utc_iso(properties["time"])
    update_time = to_utc_iso(properties.get("lastupdate", properties["time"]))
    coordinates = data["geometry"]["coordinates"]
    return Record(
        event_id=make_event_id("seismic", properties.get("unid", data["id"]), update_time),
        source="seismic",
        event_time=event_time,
        ingestion_time=ingestion_time or now_utc_iso(),
        location={"lat": coordinates[1], "lon": coordinates[0], "depth": abs(coordinates[2])},
        payload={"event": event},
    )


def send_message(event: dict) -> str:
    """Publish one EMSC event and return its message ID."""
    record = parse(event)
    body = record.to_json()
    if TOPIC_ARN:
        response = sns.publish(
            TopicArn=TOPIC_ARN,
            Message=body,
            MessageGroupId=record.message_group_id,
            MessageDeduplicationId=record.deduplication_id,
        )
        return response["MessageId"]

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=body,
        MessageGroupId=record.message_group_id,
        MessageDeduplicationId=record.deduplication_id,
    )
    return response["MessageId"]


async def stream_events() -> AsyncIterator[dict]:
    """Yield every message from the EMSC feed as the full parsed JSON dict.

    Reconnects automatically (with backoff) if the connection drops.
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
    async for event in stream_events():
        # boto3 is blocking; run it in a thread so websocket pings aren't delayed.
        message_id = await asyncio.to_thread(send_message, event)
        logging.info(
            "Sent %s event %s as message %s",
            event["action"],
            event["data"]["id"],
            message_id,
        )


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Close WebSocket")
