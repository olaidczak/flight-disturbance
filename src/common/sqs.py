"""Publish records to SNS FIFO or the legacy SQS FIFO queue.

TOPIC_ARN is preferred. QUEUE_URL remains as a migration fallback.
Without either variable, records are printed for local development.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

BATCH_SIZE = 10


def _entries(records) -> list[dict]:
    return [
        {
            "Id": str(i),
            "Message": record.to_json(),
            "MessageGroupId": record.message_group_id,
            "MessageDeduplicationId": record.deduplication_id,
        }
        for i, record in enumerate(records)
    ]


def publish(records) -> int:
    """Publish records and return their count."""
    topic_arn = os.environ.get("TOPIC_ARN")
    url = os.environ.get("QUEUE_URL")
    if not topic_arn and not url:
        logger.info("TOPIC_ARN and QUEUE_URL are not set; printing %d records", len(records))
        for r in records[:3]:
            print(r.to_json())
        return len(records)

    import boto3

    if topic_arn:
        sns = boto3.client("sns")
        for i in range(0, len(records), BATCH_SIZE):
            response = sns.publish_batch(
                TopicArn=topic_arn,
                PublishBatchRequestEntries=_entries(records[i : i + BATCH_SIZE]),
            )
            if response.get("Failed"):
                raise RuntimeError(f"SNS rejected messages: {response['Failed']}")
        return len(records)

    sqs = boto3.client("sqs")
    for i in range(0, len(records), BATCH_SIZE):
        entries = _entries(records[i : i + BATCH_SIZE])
        for entry in entries:
            entry["MessageBody"] = entry.pop("Message")
        resp = sqs.send_message_batch(QueueUrl=url, Entries=entries)
        if resp.get("Failed"):
            raise RuntimeError(f"SQS rejected messages: {resp['Failed']}")
    return len(records)
