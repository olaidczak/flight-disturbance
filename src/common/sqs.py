"""Publikacja rekordów do SQS FIFO.

Bez zmiennej QUEUE_URL (tryb lokalny) tylko wypisuje kilka rekordów, więc
pollery można odpalać na laptopie bez AWS.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

BATCH_SIZE = 10  # limit SQS: 10 wiadomości na SendMessageBatch


def publish(records) -> int:
    """Wysyła rekordy do kolejki. Zwraca liczbę rekordów."""
    url = os.environ.get("QUEUE_URL")
    if not url:
        logger.info("QUEUE_URL nie ustawione - tryb lokalny, nic nie wysyłam (%d rekordów)", len(records))
        for r in records[:3]:
            print(r.to_json())
        return len(records)

    import boto3  # import tutaj, żeby testy lokalne nie wymagały konfiguracji AWS

    sqs = boto3.client("sqs")
    for i in range(0, len(records), BATCH_SIZE):
        entries = [
            {
                "Id": str(j),
                "MessageBody": r.to_json(),
                "MessageGroupId": r.message_group_id,
                "MessageDeduplicationId": r.deduplication_id,
            }
            for j, r in enumerate(records[i : i + BATCH_SIZE])
        ]
        resp = sqs.send_message_batch(QueueUrl=url, Entries=entries)
        if resp.get("Failed"):
            raise RuntimeError(f"SQS odrzuciło wiadomości: {resp['Failed']}")
    return len(records)
