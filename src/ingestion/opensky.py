"""Poller OpenSky: stany samolotów w prostokącie (domyślnie Polska).

BRONZE = surowe dane. payload["state"] to tablica z API dokładnie taka, jak
przyszła (bez nazw pól, bez obcinania callsign, z nullami). Nazwanie pól
(FIELDS) i czyszczenie robi konsument => silver. Poller czyta z tablicy tylko
te pola, które są potrzebne do koperty: icao24, last_contact, lat, lon.

Jedyne odrzucenie: stan bez icao24 albo bez czasu, bo bez nich nie da się
zbudować event_id. Liczba takich stanów jest logowana i zwracana jako `skipped`.

Duplikaty: event_id = icao24 + last_contact, więc samolot, który nie nadał nic
nowego między dwiema migawkami, daje ten sam event_id i jest odrzucany przez
SQS FIFO / nadpisywany pod tym samym kluczem S3.

Uwierzytelnianie: OAuth2 client credentials (client_id + client_secret z konta OpenSky).
Bez poświadczeń działa anonimowo (400 kredytów dziennie, tylko do testów).
Koszt zapytania zależy od pola prostokąta: <=25 st.kw. 1 kredyt, 25-100: 2, 100-400: 3, więcej: 4.
Domyślny prostokąt na Polskę (ok. 61 st.kw.) kosztuje 2 kredyty.
Zweryfikujcie to nagłówkiem X-Rate-Limit-Remaining (loguje go lambda_handler).
"""
from __future__ import annotations

import logging
import os
import time

import requests

from common.schema import Record, make_event_id, now_utc_iso, to_utc_iso
from common.secrets import opensky_credentials
from common.sqs import publish

logger = logging.getLogger(__name__)

API_URL = "https://opensky-network.org/api/states/all"
# UWAGA: adres tokena podany z pamięci - sprawdź w aktualnej dokumentacji OpenSky.
TOKEN_URL = os.environ.get(
    "OPENSKY_TOKEN_URL",
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
)
# lamin, lomin, lamax, lomax
BBOX = [float(x) for x in os.environ.get("OPENSKY_BBOX", "48.9,14.0,54.9,24.2").split(",")]
TIMEOUT = 20

# Kolejność pól w wektorze stanu (z extended=1 jest 18 pól).
# Poller z tego NIE korzysta do payloadu; to słownik dla konsumenta silver.
FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]
_IDX = {name: i for i, name in enumerate(FIELDS)}

_token: dict = {"value": None, "expires_at": 0.0}  # cache na czas życia rozgrzanej Lambdy


def get_token(client_id: str, client_secret: str) -> str:
    if _token["value"] and time.time() < _token["expires_at"] - 60:
        return _token["value"]
    r = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    _token["value"] = body["access_token"]
    _token["expires_at"] = time.time() + int(body.get("expires_in", 1800))  # token ważny ok. 30 min
    return _token["value"]


def fetch() -> requests.Response:
    lamin, lomin, lamax, lomax = BBOX
    headers = {"User-Agent": "flight-disruption-student-project"}
    creds = opensky_credentials()
    if creds:
        headers["Authorization"] = f"Bearer {get_token(*creds)}"
    else:
        logger.warning("Brak poświadczeń OpenSky - zapytanie anonimowe (limit 400 kredytów/dzień)")
    return requests.get(
        API_URL,
        params={"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax, "extended": 1},
        headers=headers,
        timeout=TIMEOUT,
    )


def _field(state: list, name: str):
    """Odczyt jednego pola z tablicy stanu WYŁĄCZNIE do koperty."""
    i = _IDX[name]
    return state[i] if i < len(state) else None


def parse(body: dict, ingestion_time: str) -> list[Record]:
    """Jedna odpowiedź = wiele rekordów (po jednym na samolot). Payload = surowa tablica."""
    snapshot_time = body.get("time")
    out = []
    for state in body.get("states") or []:
        icao24 = _field(state, "icao24")
        # time_position bywa null, last_contact jest zawsze; w ostateczności czas migawki
        ts = _field(state, "last_contact") or snapshot_time
        if not icao24 or ts is None:
            continue  # bez icao24 i czasu nie da się zbudować event_id
        lat, lon = _field(state, "latitude"), _field(state, "longitude")
        location = {"lat": lat, "lon": lon} if lat is not None and lon is not None else None
        out.append(
            Record(
                event_id=make_event_id("opensky", icao24, ts),
                source="opensky",
                event_time=to_utc_iso(ts),
                ingestion_time=ingestion_time,
                location=location,
                payload={"state": state, "snapshot_time": snapshot_time},  # bez zmian
            )
        )
    return out


def lambda_handler(event=None, context=None) -> dict:
    now = now_utc_iso()
    resp = fetch()
    if resp.status_code == 429:  # skończyły się kredyty - nie wywalamy Lambdy, następne wywołanie spróbuje znów
        retry = resp.headers.get("X-Rate-Limit-Retry-After-Seconds")
        logger.warning("OpenSky 429: limit kredytów, ponowić za %s s", retry)
        return {"sent": 0, "skipped": 0, "rate_limited": True, "retry_after": retry}
    resp.raise_for_status()
    logger.info("Pozostałe kredyty: %s", resp.headers.get("X-Rate-Limit-Remaining"))
    body = resp.json()
    records = parse(body, now)
    skipped = len(body.get("states") or []) - len(records)
    if skipped:
        logger.warning("OpenSky: pominięto %d stanów bez icao24 lub czasu", skipped)
    return {"sent": publish(records), "skipped": skipped, "rate_limited": False}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(lambda_handler())
