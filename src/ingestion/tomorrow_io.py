"""Poller Tomorrow.io: aktualna pogoda (endpoint realtime) dla wybranych lotnisk.

Limity planu darmowego: 3 zapytania/s, 25/godz., 500/dzień - NA KLUCZ.
Każde lotnisko to osobne zapytanie, więc obowiązuje wzór:
    liczba_lotnisk * 60 / odstęp_w_minutach  <=  25 * liczba_kluczy
Przy 5 lotniskach i 2 kluczach wystarczy wywołanie co 15 min (20 zapytań/godz., 480/dzień).
Zapytania rozkładamy na klucze naprzemiennie (lotnisko i -> klucz i % liczba_kluczy).
"""
from __future__ import annotations

import logging
import time

import requests

from common.schema import Record, make_event_id, now_utc_iso, to_utc_iso
from common.secrets import tomorrow_api_keys
from common.sqs import publish

logger = logging.getLogger(__name__)

API_URL = "https://api.tomorrow.io/v4/weather/realtime"
TIMEOUT = 15
PAUSE_S = 0.4  # limit 3 zapytania/s

# kod ICAO -> (lat, lon). Współrzędne przybliżone - dopasujcie do waszego zakresu projektu.
LOCATIONS = {
    "EPWA": (52.166, 20.967),  # Warszawa Chopin
    "EPKK": (50.078, 19.785),  # Kraków
    "EPGD": (54.378, 18.466),  # Gdańsk
    "EPWR": (51.103, 16.886),  # Wrocław
    "EPPO": (52.421, 16.826),  # Poznań
}


def fetch(lat: float, lon: float, api_key: str) -> requests.Response:
    return requests.get(
        API_URL,
        params={"location": f"{lat},{lon}", "apikey": api_key, "units": "metric"},
        timeout=TIMEOUT,
    )


def parse(body: dict, station: str, ingestion_time: str) -> Record:
    data = body["data"]
    loc = body["location"]
    event_time = to_utc_iso(data["time"])
    lat, lon = loc["lat"], loc["lon"]
    return Record(
        event_id=make_event_id("tomorrow_io", f"{station}", event_time),
        source="tomorrow_io",
        event_time=event_time,
        ingestion_time=ingestion_time,
        location={"lat": lat, "lon": lon},
        payload={"station": station, **data["values"]},
    )


def lambda_handler(event=None, context=None) -> dict:
    now = now_utc_iso()
    keys = tomorrow_api_keys()
    records, errors = [], []

    for i, (station, (lat, lon)) in enumerate(LOCATIONS.items()):
        key = keys[i % len(keys)]
        try:
            resp = fetch(lat, lon, key)
            if resp.status_code == 429:
                logger.warning("Tomorrow.io 429 dla %s (limit klucza %d)", station, i % len(keys))
                errors.append(f"{station}: 429")
                continue
            resp.raise_for_status()
            records.append(parse(resp.json(), station, now))
        except Exception as e:  # noqa: BLE001 - jedno lotnisko nie zatrzymuje reszty
            logger.exception("Tomorrow.io %s: błąd", station)
            errors.append(f"{station}: {e!r}")
        time.sleep(PAUSE_S)

    sent = publish(records)
    if errors and not records:
        raise RuntimeError("; ".join(errors))
    return {"sent": sent, "errors": errors}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(lambda_handler())
