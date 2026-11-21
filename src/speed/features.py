"""Build small, deterministic feature sets from transformed records."""
from __future__ import annotations

import math
from datetime import datetime

STATIONS = {
    "EPWA": (52.166, 20.967),
    "EPKK": (50.078, 19.785),
    "EPGD": (54.378, 18.466),
    "EPWR": (51.103, 16.886),
    "EPPO": (52.421, 16.826),
}


def _pick(data: dict, names: list[str]) -> dict:
    return {name: data.get(name) for name in names if name in data}


def light_features(source: str, data: dict) -> dict:
    if source == "opensky":
        return _pick(data, [
            "velocity", "baro_altitude", "geo_altitude", "vertical_rate",
            "on_ground", "true_track", "latitude", "longitude",
        ])
    if source == "tomorrow_io":
        return _pick(data, [
            "temperature", "wind_speed", "wind_gust", "wind_direction",
            "visibility", "rain_intensity", "snow_intensity", "sleet_intensity",
            "cloud_cover", "weather_code",
        ])
    if source == "noaa_swpc":
        return _pick(data, [
            "series", "satellite", "kp", "proton_speed", "proton_density",
            "bt", "bx_gsm", "by_gsm", "bz_gsm",
        ])
    if source == "seismic":
        return _pick(data, ["magnitude", "depth", "region", "event_type"])
    return {}


def nearest_station(location: dict | None) -> tuple[str | None, float | None]:
    if not location:
        return None, None
    lat = float(location["lat"])
    lon = float(location["lon"])
    station, distance = min(
        ((code, _distance_km(lat, lon, station_lat, station_lon))
         for code, (station_lat, station_lon) in STATIONS.items()),
        key=lambda item: item[1],
    )
    return station, round(distance, 1)


def age_minutes(newer_time: str, older_time: str | None) -> float | None:
    if not older_time:
        return None
    newer = datetime.fromisoformat(newer_time.replace("Z", "+00:00"))
    older = datetime.fromisoformat(older_time.replace("Z", "+00:00"))
    return max(0.0, round((newer - older).total_seconds() / 60, 1))


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
