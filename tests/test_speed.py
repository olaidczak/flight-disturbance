"""Unit tests for the Speed Layer (`src/speed/features.py` and `src/speed/handler.py`).

Tests run completely offline against an in-memory FakeTable mimicking DynamoDB
(including conditional `version_time` checks for out-of-order updates) without
calling AWS.

Run from repository root:
    PYTHONPATH=src pytest -q tests/test_speed.py
"""
from __future__ import annotations

import copy
import json
import sys
import types
from decimal import Decimal

import pytest

from common.schema import Record
from ingestion import noaa_swpc, opensky, tomorrow_io
from speed import features, handler

NOW = "2026-11-21T12:00:00Z"

STATE = [
    "4952c7", "LOT123  ", "Poland", 1795175990, 1795176000, 21.0, 52.2, 10000.0, False,
    230.0, 90.0, 0.0, None, 10100.0, "1000", False, 0, 3,
]

SEISMIC_MSG = {
    "action": "create",
    "data": {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-80.4598, 7.5292, -15.0]},
        "id": "20261115_0000312",
        "properties": {
            "source_id": "2090757",
            "source_catalog": "EMSC-RTS",
            "lastupdate": "2026-11-16T19:29:23.33162Z",
            "time": "2026-11-15T05:08:52.0Z",
            "flynn_region": "PANAMA",
            "lat": 7.5292,
            "lon": -80.4598,
            "depth": 15.0,
            "evtype": "ke",
            "auth": "IGC",
            "mag": 3.2,
            "magtype": "m",
            "unid": "20261115_0000312",
        },
    },
}


# ---------------------------------------------------------- in-memory DynamoDB
class ConditionalCheckFailedException(Exception):
    pass


class FakeTable:
    """In-memory DynamoDB Table stub supporting `get_item` and conditional `put_item`."""

    def __init__(self):
        self.items: dict[tuple[str, str], dict] = {}
        exceptions = types.SimpleNamespace(
            ConditionalCheckFailedException=ConditionalCheckFailedException,
        )
        self.meta = types.SimpleNamespace(
            client=types.SimpleNamespace(exceptions=exceptions),
        )

    def get_item(self, Key: dict) -> dict:
        item = self.items.get((Key["entity_id"], Key["record_type"]))
        return {"Item": copy.deepcopy(item)} if item is not None else {}

    def put_item(self, Item: dict, ConditionExpression=None) -> None:
        key = (Item["entity_id"], Item["record_type"])
        existing = self.items.get(key)
        if ConditionExpression is not None and existing is not None:
            if existing["version_time"] > Item["version_time"]:
                raise ConditionalCheckFailedException("stale version_time")
        self.items[key] = copy.deepcopy(Item)


@pytest.fixture
def table(monkeypatch):
    fake = FakeTable()
    monkeypatch.setenv("REALTIME_TABLE", "flight-disruption-realtime-test")
    monkeypatch.setenv("WEATHER_MAX_AGE_MIN", "30")
    monkeypatch.setenv("NOAA_MAX_AGE_MIN", "30")
    monkeypatch.setenv("TTL_HOURS", "24")
    monkeypatch.setattr(handler, "_table", fake)

    # Provide a lightweight Attr stub if boto3 is not installed in a local test venv
    try:
        import boto3.dynamodb.conditions  # noqa: F401
    except ModuleNotFoundError:
        class _Expr:
            def __or__(self, other):
                return ("or", self, other)

        class _Attr:
            def __init__(self, name):
                self.name = name

            def not_exists(self):
                return _Expr()

            def lte(self, value):
                return _Expr()

        boto3_mod = types.ModuleType("boto3")
        ddb_mod = types.ModuleType("boto3.dynamodb")
        cond_mod = types.ModuleType("boto3.dynamodb.conditions")
        cond_mod.Attr = _Attr
        monkeypatch.setitem(sys.modules, "boto3", boto3_mod)
        monkeypatch.setitem(sys.modules, "boto3.dynamodb", ddb_mod)
        monkeypatch.setitem(sys.modules, "boto3.dynamodb.conditions", cond_mod)

    return fake


# --------------------------------------------------------------------- helpers
def _opensky_record(state=None, snapshot=1795176000) -> Record:
    return opensky.parse({"time": snapshot, "states": [state or STATE]}, NOW)[0]


def _tomorrow_record(station="EPWA", event_time="2026-11-21T11:50:00Z") -> Record:
    resp = {
        "data": {
            "time": event_time,
            "values": {
                "temperature": 4.5,
                "windSpeed": 6.2,
                "windGust": 10.1,
                "visibility": 9.5,
                "cloudCover": 75.0,
            },
        },
        "location": {"lat": 52.166, "lon": 20.967},
    }
    return tomorrow_io.parse(resp, station, NOW)


def _seismic_record(lastupdate="2026-11-16T19:29:23.33162Z", mag=3.2, action="create") -> Record:
    msg = copy.deepcopy(SEISMIC_MSG)
    msg["action"] = action
    msg["data"]["properties"]["lastupdate"] = lastupdate
    msg["data"]["properties"]["mag"] = mag
    return Record(
        event_id=f"seismic:20261115_0000312:{lastupdate}",
        source="seismic",
        event_time="2026-11-15T05:08:52Z",
        ingestion_time=NOW,
        location={"lat": 7.5292, "lon": -80.4598, "depth": 15.0},
        payload=msg,
    )


def _sqs_event(records: list[Record], bodies: list[str | None] | None = None) -> dict:
    msgs = []
    for i, rec in enumerate(records):
        body = bodies[i] if bodies and bodies[i] is not None else rec.to_json()
        msgs.append({
            "messageId": f"m{i}",
            "body": body,
            "attributes": {"MessageGroupId": rec.message_group_id},
        })
    return {"Records": msgs}


def _contains_float(obj) -> bool:
    if isinstance(obj, float):
        return True
    if isinstance(obj, dict):
        return any(_contains_float(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_float(v) for v in obj)
    return False


# ============================================================ speed.features
def test_light_features_extracts_per_source_fields():
    assert features.light_features("opensky", {
        "icao24": "4952c7", "velocity": 230.0, "baro_altitude": 10000.0, "on_ground": False, "callsign": "LOT123",
    }) == {"velocity": 230.0, "baro_altitude": 10000.0, "on_ground": False}

    assert features.light_features("tomorrow_io", {
        "station": "EPWA", "temperature": 4.5, "wind_speed": 6.2, "humidity": 80,
    }) == {"temperature": 4.5, "wind_speed": 6.2}

    assert features.light_features("noaa_swpc", {
        "series": "wind", "satellite": "DSCOVR", "proton_speed": 420.5, "ignored_col": 1,
    }) == {"series": "wind", "satellite": "DSCOVR", "proton_speed": 420.5}

    assert features.light_features("seismic", {
        "mag": 3.2, "depth": 15.0, "flynn_region": "PANAMA", "evtype": "ke",
    }) == {"magnitude": 3.2, "depth": 15.0, "region": "PANAMA", "event_type": "ke"}

    assert features.light_features("unknown", {"x": 1}) == {}


def test_nearest_station_selects_closest_airport_and_handles_none():
    station_waw, dist_waw = features.nearest_station({"lat": 52.2, "lon": 21.0})
    assert station_waw == "EPWA"
    assert dist_waw < 10.0

    station_krk, dist_krk = features.nearest_station({"lat": 50.06, "lon": 19.94})
    assert station_krk == "EPKK"
    assert dist_krk < 20.0

    assert features.nearest_station(None) == (None, None)


def test_age_minutes_computes_elapsed_time_and_clamps_negative():
    assert features.age_minutes("2026-11-21T12:00:00Z", "2026-11-21T11:45:00Z") == 15.0
    assert features.age_minutes("2026-11-21T11:45:00Z", "2026-11-21T12:00:00Z") == 0.0
    assert features.age_minutes("2026-11-21T12:00:00Z", None) is None


# ============================================================= speed.handler
def test_process_writes_weather_noaa_seismic_with_decimal_types(table):
    weather = _tomorrow_record()
    wind = noaa_swpc.parse_rtsw(
        [{"time_tag": "2026-11-21T11:55:00", "source": "DSCOVR", "active": True, "proton_speed": 410.5}],
        "wind", NOW, since="2026-11-21T11:00:00Z",
    )[0]
    quake = _seismic_record()

    assert handler.process(weather.to_json()) == "written"
    assert handler.process(wind.to_json()) == "written"
    assert handler.process(quake.to_json()) == "written"

    assert ("weather#EPWA", "LATEST") in table.items
    assert ("noaa#wind", "LATEST") in table.items
    assert ("seismic#20261115_0000312", "LATEST") in table.items

    for item in table.items.values():
        assert not _contains_float(item), "DynamoDB items must use Decimal instead of raw float"
        assert item["transform_version"] == handler.TRANSFORM_VERSION
        assert item["feature_version"] == handler.FEATURE_VERSION
        assert isinstance(item["expires_at"], int)

    assert table.items[("weather#EPWA", "LATEST")]["features"]["wind_speed"] == Decimal("6.2")
    assert table.items[("seismic#20261115_0000312", "LATEST")]["features"]["magnitude"] == Decimal("3.2")


def test_process_opensky_enriches_flight_with_fresh_weather_and_noaa_context(table):
    # OpenSky STATE timestamp 1795176000 == 2026-11-21T12:00:00Z
    flight = _opensky_record()
    flight_time = flight.event_time

    weather = _tomorrow_record("EPWA", event_time=flight_time)
    wind = noaa_swpc.parse_rtsw(
        [{"time_tag": flight_time.rstrip("Z"), "source": "DSCOVR", "active": True, "proton_speed": 420.0}],
        "wind", NOW, since="2020-01-01T00:00:00Z",
    )[0]
    mag = noaa_swpc.parse_rtsw(
        [{"time_tag": flight_time.rstrip("Z"), "source": "DSCOVR", "active": True, "bz_gsm": -5.1}],
        "mag", NOW, since="2020-01-01T00:00:00Z",
    )[0]
    kp = noaa_swpc.parse_kp(
        [{"time_tag": flight_time.rstrip("Z"), "Kp": 4.0, "a_running": 18, "station_count": 8}],
        NOW,
    )[0]

    for rec in (weather, wind, mag, kp):
        assert handler.process(rec.to_json()) == "written"

    assert handler.process(flight.to_json()) == "written"

    item = table.items[("flight#4952c7", "LATEST")]
    assert item["features"]["nearest_weather_station"] == "EPWA"
    assert item["features"]["weather_station_distance_km"] < Decimal("10")
    assert item["features"]["weather_age_minutes"] == Decimal("0.0")
    assert item["features"]["weather_stale"] is False
    assert item["features"]["noaa_stale"] is False
    assert item["features"]["missing_features"] == []
    assert item["context"]["weather"]["wind_speed"] == Decimal("6.2")
    assert item["context"]["noaa_kp"]["kp"] == Decimal("4.0")


def test_process_opensky_marks_stale_and_missing_context(table):
    flight = _opensky_record()
    # Weather from 2 hours before the flight -> stale (> 30 min); no NOAA records -> missing
    old_weather = _tomorrow_record("EPWA", event_time="2026-11-20T08:00:00Z")
    handler.process(old_weather.to_json())

    assert handler.process(flight.to_json()) == "written"
    item = table.items[("flight#4952c7", "LATEST")]
    assert item["features"]["weather_stale"] is True
    assert item["features"]["noaa_stale"] is True
    assert set(item["features"]["missing_features"]) == {"noaa_wind", "noaa_mag", "noaa_kp"}


def test_process_skips_inactive_noaa_satellite(table):
    inactive = Record(
        event_id="noaa_swpc:wind:ACE:2026-11-21T11:55:00Z",
        source="noaa_swpc",
        event_time="2026-11-21T11:55:00Z",
        ingestion_time=NOW,
        location=None,
        payload={"series": "wind", "source": "ACE", "active": False, "proton_speed": 390.0},
    )
    assert handler.process(inactive.to_json()) == "skipped"
    assert table.items == {}


def test_process_rejects_out_of_order_stale_seismic_update(table):
    newer = _seismic_record(lastupdate="2026-11-16T20:00:00.5Z", mag=3.6, action="update")
    older = _seismic_record(lastupdate="2026-11-16T19:29:23.1Z", mag=3.2, action="create")

    assert handler.process(newer.to_json()) == "written"
    assert handler.process(older.to_json()) == "stale"

    stored = table.items[("seismic#20261115_0000312", "LATEST")]
    assert stored["features"]["magnitude"] == Decimal("3.6")
    assert stored["version_time"] == "2026-11-16T20:00:00.5Z"


def test_lambda_handler_partial_batch_failure_isolates_failed_fifo_group(table):
    ok_flight = _opensky_record()
    bad_flight = _opensky_record()
    blocked_same_group = _opensky_record(state=["abc123"] + STATE[1:])
    other_group = _tomorrow_record("EPWA")

    event = _sqs_event(
        [ok_flight, bad_flight, blocked_same_group, other_group],
        bodies=[None, "{invalid json", None, None],
    )
    out = handler.lambda_handler(event)

    assert out == {"batchItemFailures": [{"itemIdentifier": "m1"}, {"itemIdentifier": "m2"}]}
    assert ("flight#4952c7", "LATEST") in table.items
    assert ("weather#EPWA", "LATEST") in table.items
    assert ("flight#abc123", "LATEST") not in table.items
