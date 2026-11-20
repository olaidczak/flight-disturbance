"""Poller NOAA SWPC: wiatr słoneczny (wind), pole magnetyczne (mag), indeks Kp.

Źródło bez klucza i bez limitów. Pliki rtsw_* mają ok. 3 MB i dobę historii, więc
bierzemy tylko wiersze z ostatnich WINDOW_MIN minut. Duplikaty z nakładających się
okien odrzuca SQS (5 min) oraz deterministyczny klucz S3 w archiverze.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import requests

from common.schema import Record, make_event_id, now_utc_iso, to_utc_iso
from common.sqs import publish

logger = logging.getLogger(__name__)

BASE = "https://services.swpc.noaa.gov"
URLS = {
    "wind": f"{BASE}/json/rtsw/rtsw_wind_1m.json",
    "mag": f"{BASE}/json/rtsw/rtsw_mag_1m.json",
    "kp": f"{BASE}/products/noaa-planetary-k-index.json",
}
WINDOW_MIN = int(os.environ.get("WINDOW_MIN", "10"))  # powinno być > odstępu między wywołaniami
TIMEOUT = 20


def fetch(url: str) -> list[dict]:
    r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "flight-disruption-student-project"})
    r.raise_for_status()
    return r.json()


def parse_rtsw(rows: list[dict], kind: str, ingestion_time: str, since: str) -> list[Record]:
    """wind/mag: tylko wiersze aktywnego satelity, nowsze niż `since` (ISO UTC)."""
    out = []
    for row in rows:
        if not row.get("active"):
            continue
        event_time = to_utc_iso(row["time_tag"])  # time_tag bez 'Z' to UTC
        if event_time < since:
            continue
        out.append(
            Record(
                event_id=make_event_id("noaa_swpc", kind, event_time),
                source="noaa_swpc",
                event_time=event_time,
                ingestion_time=ingestion_time,
                location=None,  # dane globalne, bez lokalizacji
                payload={"series": kind, **{k: v for k, v in row.items() if k not in ("time_tag", "active")}},
            )
        )
    return out


def parse_kp(rows: list[dict], ingestion_time: str) -> list[Record]:
    """Kp zmienia się co 3 h i ostatni wiersz bywa korygowany, więc bierzemy tylko najnowszy,
    a wartość Kp wchodzi do event_id (korekta = nowy rekord, nie odrzucony duplikat)."""
    if not rows:
        return []
    row = max(rows, key=lambda r: r["time_tag"])
    event_time = to_utc_iso(row["time_tag"])
    return [
        Record(
            event_id=make_event_id("noaa_swpc", "kp", event_time, row.get("Kp")),
            source="noaa_swpc",
            event_time=event_time,
            ingestion_time=ingestion_time,
            location=None,
            payload={"series": "kp", **{k: v for k, v in row.items() if k != "time_tag"}},
        )
    ]


def lambda_handler(event=None, context=None) -> dict:
    now = now_utc_iso()
    since = (datetime.now(timezone.utc) - timedelta(minutes=WINDOW_MIN)).strftime("%Y-%m-%dT%H:%M:%SZ")
    records: list[Record] = []
    errors = []

    for kind, url in URLS.items():  # jedno źródło może paść, reszta leci dalej
        try:
            rows = fetch(url)
            records += parse_kp(rows, now) if kind == "kp" else parse_rtsw(rows, kind, now, since)
        except Exception as e:  # noqa: BLE001
            logger.exception("NOAA %s: błąd", kind)
            errors.append(f"{kind}: {e!r}")

    sent = publish(records)
    if errors and not records:
        raise RuntimeError("; ".join(errors))  # wszystko padło => niech Lambda zgłosi błąd
    return {"sent": sent, "errors": errors}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(lambda_handler())
