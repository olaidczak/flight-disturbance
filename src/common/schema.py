"""Wspólny kontrakt rekordu dla wszystkich źródeł (src/common/schema.py).

Każdy poller / konsument zamienia dane źródła na `Record`. Reszta systemu
(SQS, archiver, ML) zna tylko ten kształt.

Źródło        event_time                  location        event_id
------------  --------------------------  --------------  ------------------------------------
opensky       last_contact (idx 4)        lat/lon         opensky:{icao24}:{last_contact}
tomorrow_io   data.time                   location.lat/lon tomorrow_io:{lat},{lon}:{time}
seismic       properties.time             lat/lon/depth   seismic:{unid}:{lastupdate}
noaa_swpc     time_tag (+Z, to UTC)       None (globalne) noaa_swpc:{wind|mag|kp}:{time_tag}
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

SOURCES = {"opensky", "tomorrow_io", "seismic", "noaa_swpc"}


# ---------------------------------------------------------------- czas (UTC)
def to_utc_iso(value: Any) -> str:
    """Zamienia czas ze źródła na ISO 8601 w UTC, np. '2026-11-16T10:20:00Z'.

    Przyjmuje: epoch (int/float, sekundy), string ISO z 'Z' lub z offsetem,
    string ISO bez strefy (NOAA: traktowany jako UTC).
    """
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)  # Py3.11+ radzi sobie z ułamkami sekund
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
    else:
        raise ValueError(f"Nieobsługiwany format czasu: {value!r}")
    return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -------------------------------------------------------------------- rekord
@dataclass
class Record:
    event_id: str                      # stabilny, unikalny (patrz tabela wyżej)
    source: str                        # jedno z SOURCES
    event_time: str                    # kiedy zdarzenie zaszło (UTC, ISO)
    ingestion_time: str                # kiedy je pobraliśmy (UTC, ISO)
    location: dict[str, float] | None  # {"lat":..,"lon":..,["depth":..]} lub None
    payload: dict[str, Any] = field(default_factory=dict)  # pola specyficzne dla źródła

    def __post_init__(self) -> None:
        if self.source not in SOURCES:
            raise ValueError(f"Nieznane źródło: {self.source}")
        if not self.event_id.startswith(f"{self.source}:"):
            raise ValueError(f"event_id musi zaczynać się od '{self.source}:'")
        # walidacja formatu czasu (rzuca, jeśli zły)
        for t in (self.event_time, self.ingestion_time):
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", t):
                raise ValueError(f"Czas nie jest w formacie UTC ISO: {t!r}")
        if self.location is not None and not {"lat", "lon"} <= self.location.keys():
            raise ValueError("location musi mieć klucze 'lat' i 'lon' (albo być None)")

    # --- serializacja (body wiadomości SQS i obiekt w S3) ---
    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str | bytes) -> "Record":
        return cls(**json.loads(s))

    # --- pola wymagane przez SQS FIFO ---
    @property
    def message_group_id(self) -> str:
        # Kolejność gwarantowana tylko w obrębie grupy. Grupa per źródło = prosto,
        # ale konsumenci jednej grupy działają szeregowo. Dla OpenSky rozważ
        # grupowanie po regionie, jeśli przepustowość będzie problemem.
        return self.source

    @property
    def deduplication_id(self) -> str:
        # SQS: max 128 znaków; hash jest krótki i stały.
        return hashlib.sha256(self.event_id.encode()).hexdigest()

    # --- deterministyczny klucz S3 (idempotentne zapisy) ---
    @property
    def s3_key_bronze(self) -> str:
        day = self.event_time[:10]
        safe_id = re.sub(r"[^A-Za-z0-9._=-]", "_", self.event_id)
        return f"bronze/source={self.source}/date={day}/{safe_id}.json"


# ------------------------------------------------------ pomocnicze budowanie id
def make_event_id(source: str, *parts: Any) -> str:
    """make_event_id('opensky', '4952c7', 1794824057) -> 'opensky:4952c7:1794824057'"""
    return ":".join([source, *map(str, parts)])