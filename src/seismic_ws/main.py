import asyncio
import hashlib
import json
import logging
import os
import sys
from collections.abc import AsyncIterator

import boto3
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

URI = "wss://www.seismicportal.eu/standing_order/websocket"
PING_INTERVAL = 5

# On Fargate QUEUE_URL comes from the task definition; the default is for local runs.
# Credentials come from the task role and AWS_REGION is set automatically.
QUEUE_URL = os.environ.get("QUEUE_URL")

sqs = boto3.client("sqs")


def send_message(event: dict) -> str:
    """Send the full event JSON to the FIFO queue and return the MessageId."""
    body = json.dumps(event)
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=body,
        # Updates of the same earthquake stay in order; different quakes don't block each other.
        MessageGroupId=event["data"]["id"],
        # Identical messages (e.g. two tasks overlapping during a deploy) are dropped,
        # while later updates of the same quake still go through.
        MessageDeduplicationId=hashlib.sha256(body.encode()).hexdigest(),
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
