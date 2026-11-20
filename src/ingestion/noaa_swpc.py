"""Poller NOAA SWPC: wiatr słoneczny (wind), pole magnetyczne (mag), indeks Kp.

BRONZE = surowe dane. Payload to wiersz źródła dokładnie taki, jak zwróciło API
(plus etykieta `series` mówiąca, z którego pliku pochodzi). Poller niczego nie
czyści: nie wybiera aktywnego satelity, nie zmienia nazw ani typów pól, nie
usuwa nulli. Koperta (event_id, event_time, ingestion_time, source, location)
to kontrakt z common/schema.py, a nie czyszczenie. Czyszczenie robi konsument (silver).

Jedyna selekcja przed SQS to zakres pobrania, a nie przetwarzanie wartości:
- wind/mag: pliki rtsw_* mają ok. 3 MB i dobę historii, więc wysyłamy wiersze
  z ostatnich WINDOW_MIN minut (wszystkie satelity, także nieaktywne).
- kp: tylko najnowszy wiersz (zmienia się co 3 h). Wartość Kp jest w event_id,
  więc korekta tego samego time_tag to nowy rekord, a nie odrzucony duplikat.

Okno i duplikaty:
- Poller jest bezstanowy. Okna kolejnych wywołań nakładają się, więc ten sam
  wiersz leci kilka razy (ok. WINDOW_MIN / odstęp wywołań). To zamierzone:
  at-least-once + idempotencja. Duplikaty odrzuca SQS FIFO (5 min, dedup id
  liczony z event_id), a to, co przejdzie, nadpisuje ten sam klucz S3 w archiverze.
- WINDOW_MIN >= odstęp wywołań + opóźnienie publikacji po stronie NOAA + zapas
  na jedno nieudane wywołanie. Przerwa dłuższa niż okno = dziura w bronze
  (nadrabia ją backfill/replay).
  Przykład: wywołanie co 2 min, opóźnienie NOAA ok. 2 min => WINDOW_MIN = 6.
  Opóźnienie da się zmierzyć z danych: ingestion_time minus event_time
  najnowszego wiersza.

Źródło bez klucza i bez limitów.
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
WINDOW_MIN = int(os.environ.get("WINDOW_MIN", "6"))  # patrz docstring: > odstęp wywołań + opóźnienie NOAA
TIMEOUT = 20


def fetch(url: str) -> list[dict]:
    r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": "flight-disruption-student-project"})
    r.raise_for_status()
    return r.json()


def parse_rtsw(rows: list[dict], kind: str, ingestion_time: str, since: str) -> list[Record]:
    """wind/mag: wszystkie wiersze (także nieaktywnych satelitów) nowsze niż `since` (ISO UTC).
    To zakres pobrania. Wybór aktywnego satelity to czyszczenie => silver, nie poller."""
    out = []
    for row in rows:
        time_tag = row.get("time_tag")
        if not time_tag:
            logger.warning("NOAA %s: wiersz bez time_tag pominięty (nie da się zbudować event_id)", kind)
            continue
        event_time = to_utc_iso(time_tag)  # time_tag bez 'Z' to UTC
        if event_time < since:
            continue
        out.append(
            Record(
                # satelita w id, bo ACE/IMAP/SOLAR1 mają ten sam time_tag
                event_id=make_event_id("noaa_swpc", kind, row.get("source"), event_time),
                source="noaa_swpc",
                event_time=event_time,
                ingestion_time=ingestion_time,
                location=None,  # dane globalne, bez lokalizacji
                payload={"series": kind, **row},  # wiersz bez zmian + etykieta pliku
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
            payload={"series": "kp", **row},
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
