"""Archiver bronze: SQS FIFO -> S3 (bronze/).

Wyzwalacz: event source mapping z kolejki SQS FIFO (z ReportBatchItemFailures).
Body wiadomości to Record.to_json() z pollera. Archiver niczego nie zmienia:
waliduje kopertę (Record.from_json) i zapisuje rekord pod deterministycznym
kluczem Record.s3_key_bronze.

Idempotencja (at-least-once + nakładające się okna pollerów):
- klucz zależy tylko od event_id, więc duplikat trafia pod ten sam klucz,
- domyślnie zapis jest warunkowy (If-None-Match: *): pierwszy zapis wygrywa,
  więc ingestion_time w bronze to czas pierwszego pobrania rekordu.
  Gdy obiekt już jest, S3 zwraca 412 - traktujemy to jako sukces (duplikat).
  Wyłączenie: BRONZE_WRITE_ONCE=false (wtedy ostatni zapis nadpisuje).
  Wymaga boto3 z obsługą zapisów warunkowych (wersje z końca 2024 i nowsze).

Częściowe błędy partii: dla FIFO po pierwszym błędzie w grupie zwracamy tę
wiadomość i wszystkie kolejne z TEJ SAMEJ grupy, żeby nie złamać kolejności.
Wiadomości z innych grup przetwarzamy normalnie. Wadliwe wiadomości po
maxReceiveCount trafiają do DLQ kolejki.

Zmienne środowiskowe: LAKE_BUCKET (wymagana), BRONZE_WRITE_ONCE (domyślnie true).
"""
from __future__ import annotations

import logging
import os

from common.schema import Record

logger = logging.getLogger(__name__)

_s3 = None  # klient tworzony leniwie (testy podmieniają go atrapą)


def get_s3():
    global _s3
    if _s3 is None:
        import boto3  # w runtime Lambdy jest wbudowane

        _s3 = boto3.client("s3")
    return _s3


def _write_once() -> bool:
    return os.environ.get("BRONZE_WRITE_ONCE", "true").lower() != "false"


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def archive_message(body: str | bytes) -> str:
    """Zapisuje jeden rekord do bronze. Zwraca 'written' albo 'duplicate'."""
    record = Record.from_json(body)  # rzuca przy złej kopercie => wiadomość trafi do ponowienia/DLQ
    kwargs = {"IfNoneMatch": "*"} if _write_once() else {}
    try:
        get_s3().put_object(
            Bucket=os.environ["LAKE_BUCKET"],
            Key=record.s3_key_bronze,
            Body=record.to_json().encode("utf-8"),
            ContentType="application/json",
            **kwargs,
        )
    except Exception as e:  # noqa: BLE001
        if _error_code(e) in ("PreconditionFailed", "412"):
            return "duplicate"  # obiekt już istnieje: rekord zarchiwizowany wcześniej
        raise
    return "written"


def lambda_handler(event: dict, context=None) -> dict:
    failures: list[dict] = []
    failed_groups: set[str] = set()
    counts = {"written": 0, "duplicate": 0}

    for msg in event.get("Records", []):
        group = msg.get("attributes", {}).get("MessageGroupId", "")
        if group in failed_groups:
            failures.append({"itemIdentifier": msg["messageId"]})  # zachowaj kolejność w grupie
            continue
        try:
            counts[archive_message(msg["body"])] += 1
        except Exception:  # noqa: BLE001
            logger.exception("Archiver: błąd wiadomości %s (grupa %s)", msg.get("messageId"), group)
            failures.append({"itemIdentifier": msg["messageId"]})
            failed_groups.add(group)

    logger.info("Archiver: %s, błędy: %d", counts, len(failures))
    return {"batchItemFailures": failures}
