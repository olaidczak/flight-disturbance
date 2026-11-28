"""Archiver: SQS FIFO -> S3 (raw/), paczki JSONL.

Wyzwalacz: event source mapping z kolejki SQS FIFO (z ReportBatchItemFailures).
Body wiadomości to Record.to_json() z pollera. Archiver niczego nie zmienia:
waliduje kopertę (Record.from_json) i zapisuje rekordy z jednej paczki SQS (do 10
wiadomości) jako pliki JSONL, po jednym na źródło:

    raw/ingest_date=<YYYY-MM-DD>/source=<source>/<hash>.jsonl

`raw/` to tylko poczekalnia (lifecycle S3 kasuje ją po kilku dniach). Nocny job Glue
zamienia ją w Parquet (lake/bronze, lake/silver). Dzięki paczkom powstaje wielokrotnie
mniej obiektów niż przy jednym pliku na rekord.

Idempotencja (at-least-once): <hash> to SHA-256 posortowanych event_id z paczki, więc
ponowne dostarczenie tej samej paczki nadpisuje ten sam plik tą samą treścią. Duplikaty,
które mimo to powstaną (np. ponowienie po północy trafia do folderu następnego dnia,
nakładające się okna pollerów), usuwa job Glue po event_id.

Częściowe błędy partii: dla FIFO po pierwszym błędzie w grupie zwracamy tę
wiadomość i wszystkie kolejne z TEJ SAMEJ grupy, żeby nie złamać kolejności.
Wiadomości z innych grup przetwarzamy normalnie. Błąd zapisu do S3 kończy wywołanie
wyjątkiem (SQS ponawia całą paczkę; zapis jest idempotentny). Wadliwe wiadomości po
maxReceiveCount trafiają do DLQ kolejki.

Zmienne środowiskowe: LAKE_BUCKET (wymagana).
"""
from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone

from common.schema import Record

logger = logging.getLogger(__name__)

RAW_PREFIX = "raw"

_s3 = None  # klient tworzony leniwie (testy podmieniają go atrapą)


def get_s3():
    global _s3
    if _s3 is None:
        import boto3  # w runtime Lambdy jest wbudowane

        _s3 = boto3.client("s3")
    return _s3


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def raw_key(day: str, source: str, records: list[Record]) -> str:
    """Deterministyczny klucz paczki: ta sama paczka rekordów => ten sam klucz."""
    digest = hashlib.sha256("\n".join(sorted(r.event_id for r in records)).encode()).hexdigest()[:32]
    return f"{RAW_PREFIX}/ingest_date={day}/source={source}/{digest}.jsonl"


def lambda_handler(event: dict, context=None) -> dict:
    failures: list[dict] = []
    failed_groups: set[str] = set()
    by_source: dict[str, list[Record]] = {}

    for msg in event.get("Records", []):
        group = msg.get("attributes", {}).get("MessageGroupId", "")
        if group in failed_groups:
            failures.append({"itemIdentifier": msg["messageId"]})  # zachowaj kolejność w grupie
            continue
        try:
            rec = Record.from_json(msg["body"])  # rzuca przy złej kopercie
        except Exception:  # noqa: BLE001
            logger.exception("Archiver: błąd wiadomości %s (grupa %s)", msg.get("messageId"), group)
            failures.append({"itemIdentifier": msg["messageId"]})
            failed_groups.add(group)
            continue
        by_source.setdefault(rec.source, []).append(rec)

    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    written = 0
    for source, recs in by_source.items():
        recs.sort(key=lambda r: r.event_id)
        get_s3().put_object(
            Bucket=os.environ["LAKE_BUCKET"],
            Key=raw_key(day, source, recs),
            Body="\n".join(r.to_json() for r in recs).encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        written += len(recs)

    logger.info("Archiver: zapisano %d rekordów w %d plikach, błędy: %d", written, len(by_source), len(failures))
    return {"batchItemFailures": failures}
