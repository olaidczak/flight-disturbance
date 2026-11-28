"""Jawne schematy silver (Parquet). Bez importu pyspark, więc da się je testować offline.

Zasady typów:
- liczby z NOAA to zawsze DOUBLE (kod silver zamienia tekst liczbowy na float),
- liczby ciągłe (pozycje, prędkości, pogoda, magnituda) to DOUBLE,
- kody całkowite z OpenSky i Tomorrow.io to INT,
- czasy zostają tekstem ISO UTC, tak jak w kontrakcie silver.
Pole spoza schematu jest pomijane, a wartość o złym typie staje się NULL (tryb PERMISSIVE).
"""
from __future__ import annotations

import json


def _doubles(*names: str) -> list[tuple[str, str]]:
    return [(n, "DOUBLE") for n in names]


OPENSKY = [
    ("icao24", "STRING"), ("callsign", "STRING"), ("origin_country", "STRING"),
    ("time_position", "STRING"), ("last_contact", "STRING"), ("snapshot_time", "STRING"),
    ("longitude", "DOUBLE"), ("latitude", "DOUBLE"), ("baro_altitude", "DOUBLE"),
    ("on_ground", "BOOLEAN"), ("velocity", "DOUBLE"), ("true_track", "DOUBLE"),
    ("vertical_rate", "DOUBLE"), ("geo_altitude", "DOUBLE"), ("squawk", "STRING"),
    ("spi", "BOOLEAN"), ("position_source", "INT"), ("category", "INT"),
]

TOMORROW_IO = [("station", "STRING")] + _doubles(
    "altimeter_setting", "cloud_base", "cloud_ceiling", "cloud_cover", "dew_point",
    "freezing_rain_intensity", "humidity", "precipitation_probability", "pressure_sea_level",
    "pressure_surface_level", "rain_intensity", "sleet_intensity", "snow_intensity",
    "temperature", "temperature_apparent", "uv_health_concern", "uv_index", "visibility",
) + [("weather_code", "INT")] + _doubles("wind_direction", "wind_gust", "wind_speed")

SEISMIC = [
    ("action", "STRING"), ("source_id", "STRING"), ("source_catalog", "STRING"),
    ("lastupdate", "STRING"), ("time", "STRING"), ("flynn_region", "STRING"),
    ("lat", "DOUBLE"), ("lon", "DOUBLE"), ("depth", "DOUBLE"), ("evtype", "STRING"),
    ("auth", "STRING"), ("mag", "DOUBLE"), ("magtype", "STRING"), ("unid", "STRING"),
]

_VEC = ["vx_gse", "vy_gse", "vz_gse", "vx_gsm", "vy_gsm", "vz_gsm"]

NOAA_WIND = [("series", "STRING"), ("satellite", "STRING")] + _doubles(
    "proton_speed", "proton_temperature", "proton_density", *[f"proton_{v}" for v in _VEC],
    "proton_sample_size", "alpha_speed", "alpha_temperature", "alpha_density",
    *[f"alpha_{v}" for v in _VEC], "alpha_sample_size", "max_convergence_flag",
    "max_data_flag", "max_error_count_flag", "max_processing_flag", "max_range_flag",
    "max_sample_count_flag", "max_telemetry_flag", "overall_quality",
)

NOAA_MAG = [("series", "STRING"), ("satellite", "STRING")] + _doubles("range", "scale", "sensitivity") + [
    ("manual_mode", "BOOLEAN"),
] + _doubles(
    "sample_size", "bt", "bx_gse", "by_gse", "bz_gse", "theta_gse", "phi_gse",
    "bx_gsm", "by_gsm", "bz_gsm", "theta_gsm", "phi_gsm",
    "max_telemetry_flag", "max_data_flag", "overall_quality",
)

NOAA_KP = [("series", "STRING")] + _doubles("kp", "a_running", "station_count")

# (nazwa zbioru w lake/silver/, źródło, seria NOAA albo None)
DATASETS = [
    ("opensky", "opensky", None),
    ("tomorrow_io", "tomorrow_io", None),
    ("seismic", "seismic", None),
    ("noaa_swpc_wind", "noaa_swpc", "wind"),
    ("noaa_swpc_mag", "noaa_swpc", "mag"),
    ("noaa_swpc_kp", "noaa_swpc", "kp"),
]
_DATA = {"opensky": OPENSKY, "tomorrow_io": TOMORROW_IO, "seismic": SEISMIC,
         "noaa_swpc_wind": NOAA_WIND, "noaa_swpc_mag": NOAA_MAG, "noaa_swpc_kp": NOAA_KP}


def dataset_name(source: str, series: str | None = None) -> str:
    """('noaa_swpc', 'wind') -> 'noaa_swpc_wind'; dla pozostałych źródeł nazwa = źródło."""
    return f"{source}_{series}" if series else source


def data_fields(name: str) -> list[str]:
    return [n for n, _ in _DATA[name]]


def ddl(name: str) -> str:
    """Schemat całego dokumentu silver jako tekst DDL dla Sparka."""
    data = ", ".join(f"`{n}`: {t}" for n, t in _DATA[name])
    return ("event_id STRING, source STRING, event_time STRING, ingestion_time STRING, "
            "location STRUCT<lat: DOUBLE, lon: DOUBLE, depth: DOUBLE>, "
            f"data STRUCT<{data}>, silver_version INT, ingest_date STRING")


def unknown_fields(name: str, lines) -> set[str]:
    """Pola w `data`, których nie ma w schemacie (zostałyby po cichu pominięte)."""
    known = set(data_fields(name))
    seen: set[str] = set()
    for line in lines:
        seen |= set(json.loads(line)["data"])
    return seen - known
