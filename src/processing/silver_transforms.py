"""Czyszczenie bronze -> silver (czyste funkcje, bez AWS).

Wejście: Record z bronze (payload bez zmian). Wyjście: dokument silver albo None,
gdy rekord ma zostać pominięty. To jedyne miejsce, w którym zmieniamy dane źródła:

- NOAA wind/mag: odrzucamy wiersze jawnie nieaktywnych satelitów (active == False),
  `source` => `satellite`, wartości tekstowe liczbowe => liczby, usuwamy `time_tag`
  (jest w event_time). Nazwy pozostałych kolumn zostają jak w źródle.
- NOAA Kp: kp / a_running / station_count jako liczby.
- OpenSky: nazwane pola z tablicy stanu, callsign bez spacji (puste => None),
  czasy epoch => ISO UTC, odrzucone `sensors` (szum).
- Tomorrow.io: spłaszczenie response.data.values do snake_case + kod lotniska.
- Seismic: `action` + spłaszczone data.properties, `time` => ISO UTC. `lastupdate`
  zostaje z pełną precyzją: silver_handler porównuje po nim wersje tego samego
  trzęsienia (`unid`), a wersje potrafią przyjść w odstępie poniżej sekundy.
  Silver event_id = seismic:{unid} (różny od bronze, patrz common/schema.py).
- Inne źródła: payload bez zmian, dopóki nie dopiszecie transformacji.

Dokument silver ma kopertę rekordu + `data` (oczyszczone pola) + `silver_version`.
"""
from __future__ import annotations

import re
from typing import Any, Callable

from common.schema import Record, to_utc_iso

SILVER_VERSION = 1

# kolejność pól w wektorze stanu OpenSky (extended=1); test pilnuje zgodności z pollerem
OPENSKY_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]


def _coerce(value: Any) -> Any:
    """Tekst wyglądający na liczbę => float; reszta bez zmian (None, bool, liczby, inne teksty)."""
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def _snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _iso_or_none(value: Any) -> str | None:
    return None if value is None else to_utc_iso(value)


# ------------------------------------------------------------------ transformacje
def _noaa(rec: Record) -> dict | None:
    p = dict(rec.payload)
    series = p.pop("series", None)
    p.pop("time_tag", None)  # zostaje w event_time

    if series in ("wind", "mag"):
        if p.get("active") is False:
            return None  # nieaktywny satelita: wybór aktywnego to czyszczenie
        p.pop("active", None)
        satellite = p.pop("source", None)
        data = {"series": series, "satellite": satellite}
        data.update({k: _coerce(v) for k, v in p.items()})
        return data

    if series == "kp":
        return {
            "series": "kp",
            "kp": _coerce(p.get("Kp")),
            "a_running": _coerce(p.get("a_running")),
            "station_count": _coerce(p.get("station_count")),
        }

    return {"series": series, **p}  # nieznana seria: nie zgadujemy


def _opensky(rec: Record) -> dict:
    state = rec.payload["state"]
    s = {name: (state[i] if i < len(state) else None) for i, name in enumerate(OPENSKY_FIELDS)}
    callsign = (s["callsign"] or "").strip() or None
    data = {
        "icao24": s["icao24"],
        "callsign": callsign,
        "origin_country": s["origin_country"],
        "time_position": _iso_or_none(s["time_position"]),
        "last_contact": _iso_or_none(s["last_contact"]),
        "snapshot_time": _iso_or_none(rec.payload.get("snapshot_time")),
    }
    for name in OPENSKY_FIELDS:
        if name in ("icao24", "callsign", "origin_country", "time_position", "last_contact", "sensors"):
            continue
        data[name] = s[name]
    return data


def _tomorrow(rec: Record) -> dict:
    values = rec.payload["response"]["data"]["values"]
    data = {"station": rec.payload["station"]}
    data.update({_snake(k): v for k, v in values.items()})
    return data


def _seismic(rec: Record) -> dict:
    payload = rec.payload.get("event", rec.payload)
    props = payload["data"]["properties"]
    data = {"action": payload.get("action"), **props}
    data["time"] = _iso_or_none(props.get("time"))
    return data


def _passthrough(rec: Record) -> dict:
    return dict(rec.payload)


TRANSFORMS: dict[str, Callable[[Record], dict | None]] = {
    "noaa_swpc": _noaa,
    "opensky": _opensky,
    "tomorrow_io": _tomorrow,
    "seismic": _seismic,
}


def to_silver(rec: Record) -> dict | None:
    """Record (bronze) -> dokument silver albo None (rekord pominięty). Nie modyfikuje `rec`."""
    data = TRANSFORMS.get(rec.source, _passthrough)(rec)
    if data is None:
        return None
    return {
        "event_id": rec.silver_event_id,
        "source": rec.source,
        "event_time": rec.event_time,
        "ingestion_time": rec.ingestion_time,
        "location": rec.location,
        "data": data,
        "silver_version": SILVER_VERSION,
    }
