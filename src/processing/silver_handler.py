"""Lambda silver: zdarzenie S3 (nowy obiekt w bronze/) -> czyszczenie -> silver/.

Wyzwalacz: powiadomienie S3 ObjectCreated z filtrem prefix=bronze/ i suffix=.json.
Lambda czyta rekord z bronze, woła to_silver() i zapisuje wynik pod
Record.s3_key_silver. Klucz zależy tylko od event_id, więc ponowne przetworzenie
(replay, ponowienie po błędzie) nadpisuje ten sam obiekt: silver jest wyliczalny
z bronze i można go w każdej chwili przeliczyć od nowa.

Wyjątek seismic (LATEST_WINS): bronze ma każdą wersję trzęsienia, silver jeden
obiekt na trzęsienie (klucz z seismic:{unid}, różny od klucza bronze). Zapisujemy
tylko, gdy przychodząca wersja ma data.lastupdate >= zapisanej (równa = replay,
nadpisujemy tym samym). Starsza wersja (zdarzenia S3 przychodzą bez kolejności)
kończy się jako 'stale'. Zapis jest warunkowy (If-Match na ETag albo
If-None-Match, gdy obiektu nie ma), więc dwie równoległe Lambdy dla tego samego
trzęsienia nie nadpiszą nowszej wersji starszą: przegrana próba czyta obiekt
jeszcze raz. Silver jest wtedy aktualnym rozwiązaniem preferowanym przez EMSC
(najnowsze lastupdate), a nie oceną dokładności; pełna historia zostaje w bronze.

Zabezpieczenie przed pętlą: obiekty spoza prefiksu bronze/ są ignorowane
(silver/ leży w tym samym buckecie, ale nie może wyzwalać samego siebie).

Błędy: wyjątek kończy wywołanie; S3 wywołuje Lambdę asynchronicznie, więc AWS
ponawia (2 razy), a potem wysyła zdarzenie do DLQ (patrz infra/datalake_layers.tf).
"""
from __future__ import annotations

import json
import logging
from urllib.parse import unquote_plus

from datetime import datetime

from common.schema import Record
from processing.archiver import _error_code
from processing.silver_transforms import to_silver

logger = logging.getLogger(__name__)

BRONZE_PREFIX = "bronze/"
LATEST_WINS = {"seismic"}  # jeden obiekt silver na encję, wygrywa najnowszy data.lastupdate
WRITE_ATTEMPTS = 3

_s3 = None  # klient tworzony leniwie (testy podmieniają go atrapą)


def get_s3():
    global _s3
    if _s3 is None:
        import boto3

        _s3 = boto3.client("s3")
    return _s3


def _lastupdate(doc: dict) -> datetime:
    return datetime.fromisoformat(doc["data"]["lastupdate"].replace("Z", "+00:00"))


def _write_latest(s3, bucket: str, key: str, doc: dict, body: bytes) -> bool:
    """Zapisuje doc, jeśli nie jest starszy niż obiekt pod key. False = starsza wersja (pominięta)."""
    for _ in range(WRITE_ATTEMPTS):
        try:
            current = s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:  # noqa: BLE001
            if _error_code(e) not in ("NoSuchKey", "404"):
                raise
            condition = {"IfNoneMatch": "*"}
        else:
            if _lastupdate(doc) < _lastupdate(json.loads(current["Body"].read())):
                return False
            condition = {"IfMatch": current["ETag"]}
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", **condition)
            return True
        except Exception as e:  # noqa: BLE001
            # ktoś zapisał w międzyczasie: czytamy jeszcze raz i porównujemy od nowa
            if _error_code(e) not in ("PreconditionFailed", "412", "ConditionalRequestConflict", "409"):
                raise
    raise RuntimeError(f"Nie udało się zapisać {key} po {WRITE_ATTEMPTS} próbach (równoległe zapisy)")


def process_object(bucket: str, key: str) -> str:
    """Zwraca 'written', 'skipped' (rekord odrzucony przez czyszczenie), 'stale' (starsza wersja,
    tylko LATEST_WINS) albo 'ignored' (zły prefiks)."""
    if not key.startswith(BRONZE_PREFIX):
        return "ignored"
    s3 = get_s3()
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    record = Record.from_json(body)
    doc = to_silver(record)
    if doc is None:
        return "skipped"
    body = json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if record.source in LATEST_WINS:
        return "written" if _write_latest(s3, bucket, record.s3_key_silver, doc, body) else "stale"
    s3.put_object(
        Bucket=bucket,
        Key=record.s3_key_silver,
        Body=body,
        ContentType="application/json",
    )
    return "written"


def lambda_handler(event: dict, context=None) -> dict:
    counts = {"written": 0, "skipped": 0, "stale": 0, "ignored": 0}
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = unquote_plus(r["s3"]["object"]["key"])  # klucze w zdarzeniach S3 są zakodowane (np. '=' => %3D)
        counts[process_object(bucket, key)] += 1
    logger.info("Silver: %s", counts)
    return counts
