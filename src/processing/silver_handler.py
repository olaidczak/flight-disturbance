"""Lambda silver: zdarzenie S3 (nowy obiekt w bronze/) -> czyszczenie -> silver/.

Wyzwalacz: powiadomienie S3 ObjectCreated z filtrem prefix=bronze/ i suffix=.json.
Lambda czyta rekord z bronze, woła to_silver() i zapisuje wynik pod
Record.s3_key_silver. Klucz zależy tylko od event_id, więc ponowne przetworzenie
(replay, ponowienie po błędzie) nadpisuje ten sam obiekt: silver jest wyliczalny
z bronze i można go w każdej chwili przeliczyć od nowa.

Zabezpieczenie przed pętlą: obiekty spoza prefiksu bronze/ są ignorowane
(silver/ leży w tym samym buckecie, ale nie może wyzwalać samego siebie).

Błędy: wyjątek kończy wywołanie; S3 wywołuje Lambdę asynchronicznie, więc AWS
ponawia (2 razy), a potem wysyła zdarzenie do DLQ (patrz infra/datalake_layers.tf).
"""
from __future__ import annotations

import json
import logging
from urllib.parse import unquote_plus

from common.schema import Record
from processing.silver_transforms import to_silver

logger = logging.getLogger(__name__)

BRONZE_PREFIX = "bronze/"

_s3 = None  # klient tworzony leniwie (testy podmieniają go atrapą)


def get_s3():
    global _s3
    if _s3 is None:
        import boto3

        _s3 = boto3.client("s3")
    return _s3


def process_object(bucket: str, key: str) -> str:
    """Zwraca 'written', 'skipped' (rekord odrzucony przez czyszczenie) albo 'ignored' (zły prefiks)."""
    if not key.startswith(BRONZE_PREFIX):
        return "ignored"
    s3 = get_s3()
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    record = Record.from_json(body)
    doc = to_silver(record)
    if doc is None:
        return "skipped"
    s3.put_object(
        Bucket=bucket,
        Key=record.s3_key_silver,
        Body=json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return "written"


def lambda_handler(event: dict, context=None) -> dict:
    counts = {"written": 0, "skipped": 0, "ignored": 0}
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = unquote_plus(r["s3"]["object"]["key"])  # klucze w zdarzeniach S3 są zakodowane (np. '=' => %3D)
        counts[process_object(bucket, key)] += 1
    logger.info("Silver: %s", counts)
    return counts
