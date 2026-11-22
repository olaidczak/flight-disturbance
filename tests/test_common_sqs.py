"""Tests for publishing records to the shared SNS/SQS entry point."""
from __future__ import annotations

import sys
from types import SimpleNamespace

from common.schema import Record
from common.sqs import publish


def test_publish_prefers_sns_over_legacy_sqs(monkeypatch):
    calls = []

    class FakeSns:
        def publish_batch(self, **kwargs):
            calls.append(kwargs)
            return {"Failed": []}

    def client(service):
        if service == "sns":
            return FakeSns()
        raise AssertionError("Legacy SQS must not be used when TOPIC_ARN is set")

    monkeypatch.setenv("TOPIC_ARN", "arn:aws:sns:eu-north-1:123456789012:events.fifo")
    monkeypatch.setenv("QUEUE_URL", "https://sqs.eu-north-1.amazonaws.com/123456789012/events.fifo")
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=client))
    record = Record(
        event_id="opensky:abc123:1",
        source="opensky",
        event_time="2026-11-22T12:00:00Z",
        ingestion_time="2026-11-22T12:00:01Z",
        location={"lat": 52.0, "lon": 21.0},
        payload={"state": ["abc123"]},
    )

    assert publish([record]) == 1
    assert calls == [
        {
            "TopicArn": "arn:aws:sns:eu-north-1:123456789012:events.fifo",
            "PublishBatchRequestEntries": [
                {
                    "Id": "0",
                    "Message": record.to_json(),
                    "MessageGroupId": "opensky",
                    "MessageDeduplicationId": record.deduplication_id,
                }
            ],
        }
    ]
