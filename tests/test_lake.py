"""Unit tests for Data Lake layers: archiver (Bronze), silver_transforms, silver_handler,
and end-to-end flow: pollers -> archiver -> silver (using an in-memory FakeS3 stub).

Run from repository root:
    PYTHONPATH=src pytest -q tests/test_lake.py
Tests do not call AWS or live external APIs.
"""
import copy
import dataclasses
import hashlib
import importlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

import pytest

from common.schema import Record
from processing import archiver, silver_handler, silver_transforms
from processing.silver_transforms import OPENSKY_FIELDS, to_silver
from seismic_ws import main as seismic_ws


def _load_poller(name):
    """Locate a poller module regardless of package layout (src/ingestion, src/ingestion/pollers,
    or flat on PYTHONPATH). Missing internal dependencies (e.g., requests) are raised normally."""
    tried = []
    for pkg in ("ingestion", "ingestion.pollers", "pollers", ""):
        mod = f"{pkg}.{name}" if pkg else name
        tried.append(mod)
        try:
            return importlib.import_module(mod)
        except ModuleNotFoundError as e:
            if e.name and (mod == e.name or mod.startswith(e.name + ".")):
                continue  # not this package layout
            raise
    raise ImportError(f"Poller '{name}' not found. Tried: {', '.join(tried)}")


noaa_swpc = _load_poller("noaa_swpc")
opensky = _load_poller("opensky")
tomorrow_io = _load_poller("tomorrow_io")

BUCKET = "lake-test"
NOW = "2026-11-20T12:00:00Z"


# ------------------------------------------------------------------- S3 stub
class PreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class AccessDenied(Exception):
    response = {"Error": {"Code": "AccessDenied"}}


class NoSuchKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.reads: list[str] = []
        self.fail_with: Exception | None = None

    def put_object(self, Bucket, Key, Body, ContentType=None, IfNoneMatch=None, IfMatch=None):
        if self.fail_with:
            raise self.fail_with
        if IfNoneMatch == "*" and (Bucket, Key) in self.objects:
            raise PreconditionFailed()
        if IfMatch is not None and self._etag(Bucket, Key) != IfMatch:
            raise PreconditionFailed()
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        if (Bucket, Key) not in self.objects:
            raise NoSuchKey()
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)]), "ETag": self._etag(Bucket, Key)}

    def _etag(self, bucket, key):
        body = self.objects.get((bucket, key))
        return None if body is None else f'"{hashlib.md5(body).hexdigest()}"'

    def keys(self, prefix=""):
        return sorted(k for (_, k) in self.objects if k.startswith(prefix))

    def doc(self, key):
        return json.loads(self.objects[(BUCKET, key)])


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setenv("LAKE_BUCKET", BUCKET)
    monkeypatch.delenv("BRONZE_WRITE_ONCE", raising=False)
    monkeypatch.setattr(archiver, "_s3", fake)
    monkeypatch.setattr(silver_handler, "_s3", fake)
    return fake


# ------------------------------------------------------------------ helpers
def sqs_event(records, bodies=None):
    msgs = []
    for i, r in enumerate(records):
        body = bodies[i] if bodies and bodies[i] is not None else r.to_json()
        msgs.append({"messageId": f"m{i}", "body": body, "attributes": {"MessageGroupId": r.message_group_id}})
    return {"Records": msgs}


def s3_event(key, bucket=BUCKET):
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": quote_plus(key, safe="/")}}}]}


def make_record(source="opensky", event_id=None, payload=None, ingestion_time=NOW, event_time="2026-11-20T11:55:00Z"):
    return Record(
        event_id=event_id or f"{source}:x:1",
        source=source,
        event_time=event_time,
        ingestion_time=ingestion_time,
        location=None,
        payload=payload if payload is not None else {},
    )


STATE = [
    "4952c7", "LOT123  ", "Poland", 1794824050, 1794824057, 21.0, 52.2, 10000.0, False,
    230.0, 90.0, 0.0, None, 10100.0, "1000", False, 0, 3,
]


SEISMIC_MSG = json.loads((Path(__file__).parent / "seismic_sample.json").read_text())


def seismic_record(msg=None):
    return seismic_ws.parse(copy.deepcopy(msg or SEISMIC_MSG), NOW)


def opensky_record(state=None, snapshot=1794824060):
    body = {"time": snapshot, "states": [state or STATE]}
    return opensky.parse(body, NOW)[0]


# =========================================================== archiver (bronze)
def test_archiver_writes_record_unchanged_under_bronze_key(s3):
    rec = opensky_record()
    out = archiver.lambda_handler(sqs_event([rec]))

    assert out == {"batchItemFailures": []}
    assert s3.keys() == [rec.s3_key_bronze]
    assert s3.keys()[0].startswith("bronze/source=opensky/date=")
    stored = Record.from_json(s3.objects[(BUCKET, rec.s3_key_bronze)])
    assert stored == rec
    assert stored.payload["state"][1] == "LOT123  "  # callsign untouched


def test_archiver_duplicate_first_write_wins(s3):
    first = opensky_record()
    second = dataclasses.replace(first, ingestion_time="2026-11-20T12:05:00Z")

    assert archiver.lambda_handler(sqs_event([first])) == {"batchItemFailures": []}
    assert archiver.lambda_handler(sqs_event([second])) == {"batchItemFailures": []}  # 412 = success

    assert len(s3.keys()) == 1
    assert s3.doc(first.s3_key_bronze)["ingestion_time"] == NOW  # preserves first ingestion_time


def test_archiver_write_once_disabled_overwrites(s3, monkeypatch):
    monkeypatch.setenv("BRONZE_WRITE_ONCE", "false")
    first = opensky_record()
    second = dataclasses.replace(first, ingestion_time="2026-11-20T12:05:00Z")

    archiver.lambda_handler(sqs_event([first]))
    archiver.lambda_handler(sqs_event([second]))

    assert s3.doc(first.s3_key_bronze)["ingestion_time"] == "2026-11-20T12:05:00Z"


def test_archiver_message_error_blocks_same_group_only(s3):
    ok1 = opensky_record()
    bad = opensky_record()
    later_same_group = opensky_record(state=["abc123"] + STATE[1:])
    other_group = noaa_swpc.parse_kp([{"time_tag": "2026-11-20T09:00:00", "Kp": 3.0}], NOW)[0]

    event = sqs_event([ok1, bad, later_same_group, other_group], bodies=[None, "not valid json", None, None])
    out = archiver.lambda_handler(event)

    assert out["batchItemFailures"] == [{"itemIdentifier": "m1"}, {"itemIdentifier": "m2"}]
    assert ok1.s3_key_bronze in s3.keys()
    assert other_group.s3_key_bronze in s3.keys()
    assert later_same_group.s3_key_bronze not in s3.keys()  # FIFO ordering preserved within group


def test_archiver_other_s3_error_is_reported(s3):
    s3.fail_with = AccessDenied()
    out = archiver.lambda_handler(sqs_event([opensky_record()]))
    assert out["batchItemFailures"] == [{"itemIdentifier": "m0"}]


def test_archiver_rejects_invalid_envelope(s3):
    bad = json.dumps({"event_id": "bad:id", "source": "opensky", "event_time": NOW,
                      "ingestion_time": NOW, "location": None, "payload": {}})
    out = archiver.lambda_handler(sqs_event([opensky_record()], bodies=[bad]))
    assert out["batchItemFailures"] == [{"itemIdentifier": "m0"}]
    assert s3.keys() == []


# ======================================================== silver_transforms
def test_noaa_wind_active_satellite_is_cleaned():
    rec = make_record("noaa_swpc", "noaa_swpc:wind:DSCOVR:2026-11-20T11:55:00Z", {
        "series": "wind", "time_tag": "2026-11-20T11:55:00", "source": "DSCOVR", "active": True,
        "proton_speed": "410.5", "proton_density": None, "range": 1,
    })
    doc = to_silver(rec)
    assert doc["data"] == {"series": "wind", "satellite": "DSCOVR",
                           "proton_speed": 410.5, "proton_density": None, "range": 1}


def test_noaa_inactive_satellite_skipped_while_missing_active_kept():
    base = {"series": "mag", "time_tag": "2026-11-20T11:55:00", "source": "ACE", "bt": 5.0}
    inactive = make_record("noaa_swpc", "noaa_swpc:mag:ACE:t", {**base, "active": False})
    unknown = make_record("noaa_swpc", "noaa_swpc:mag:ACE:t2", dict(base))
    assert to_silver(inactive) is None
    assert to_silver(unknown)["data"]["satellite"] == "ACE"


def test_noaa_kp_transform():
    rec = make_record("noaa_swpc", "noaa_swpc:kp:2026-11-20T09:00:00Z:3.33", {
        "series": "kp", "time_tag": "2026-11-20T09:00:00", "Kp": "3.33", "a_running": 15, "station_count": 8,
    })
    assert to_silver(rec)["data"] == {"series": "kp", "kp": 3.33, "a_running": 15, "station_count": 8}


def test_opensky_named_fields_callsign_and_timestamps():
    rec = opensky_record()
    original = copy.deepcopy(rec.payload)
    doc = to_silver(rec)
    d = doc["data"]

    assert rec.payload == original, "silver must not mutate bronze record"
    assert d["icao24"] == "4952c7"
    assert d["callsign"] == "LOT123"
    iso = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    assert d["last_contact"] == iso(1794824057)
    assert d["time_position"] == iso(1794824050)
    assert d["snapshot_time"] == iso(1794824060)
    assert d["on_ground"] is False and d["velocity"] == 230.0 and d["baro_altitude"] == 10000.0
    assert "sensors" not in d
    assert doc["location"] == {"lat": 52.2, "lon": 21.0}


def test_opensky_blank_callsign_none_and_short_array():
    state = list(STATE)
    state[1] = "        "
    d = to_silver(opensky_record(state=state))["data"]
    assert d["callsign"] is None

    short = to_silver(make_record("opensky", "opensky:z:1", {"state": ["z", None, "Poland", None, 1794824057], "snapshot_time": None}))
    assert short["data"]["velocity"] is None and short["data"]["snapshot_time"] is None


def test_tomorrow_flattening_and_snake_case():
    resp = {
        "data": {"time": "2026-11-20T11:55:00Z",
                 "values": {"temperature": 3.1, "windSpeed": 5.2, "uvIndex": 0, "temperatureApparent": 1.0}},
        "location": {"lat": 52.166, "lon": 20.967, "name": "Warsaw"},
    }
    rec = tomorrow_io.parse(resp, "EPWA", NOW)
    d = to_silver(rec)["data"]
    assert d == {"station": "EPWA", "temperature": 3.1, "wind_speed": 5.2, "uv_index": 0, "temperature_apparent": 1.0}


def test_unknown_source_passes_through_unchanged(monkeypatch):
    monkeypatch.delitem(silver_transforms.TRANSFORMS, "seismic")  # source without dedicated transform
    rec = make_record("seismic", "seismic:20261120_0000001:2026-11-20T11:55:00Z", {"mag": 4.1, "region": "X"})
    assert to_silver(rec)["data"] == {"mag": 4.1, "region": "X"}


def test_seismic_flattening_properties_and_timestamps():
    rec = seismic_record()
    original = copy.deepcopy(rec.payload)
    d = to_silver(rec)["data"]

    assert rec.payload == original, "silver must not mutate bronze record"
    assert d["action"] == "create"
    assert d["unid"] == "20261115_0000312" and d["mag"] == 3.2 and d["flynn_region"] == "PANAMA"
    assert d["time"] == "2026-11-15T05:08:52Z"
    assert d["lastupdate"] == "2026-11-16T19:29:23.33162Z"  # full precision preserved for version comparison


def test_seismic_legacy_envelope_is_supported():
    rec = seismic_record()
    rec.payload = {"event": rec.payload}

    d = to_silver(rec)["data"]

    assert d["unid"] == "20261115_0000312"
    assert d["mag"] == 3.2


def test_seismic_silver_id_and_key_omit_lastupdate():
    """Exception to bronze == silver: silver stores one object per earthquake (unid)."""
    rec = seismic_record()
    assert rec.silver_event_id == "seismic:20261115_0000312"
    assert to_silver(rec)["event_id"] == "seismic:20261115_0000312"
    assert rec.s3_key_silver == "silver/source=seismic/date=2026-11-15/seismic_20261115_0000312.json"
    assert rec.s3_key_silver != rec.s3_key_bronze.replace("bronze/", "silver/", 1)


def test_silver_envelope_fields():
    rec = opensky_record()
    doc = to_silver(rec)
    for k in ("event_id", "source", "event_time", "ingestion_time", "location"):
        assert doc[k] == getattr(rec, k)
    assert doc["silver_version"] == silver_transforms.SILVER_VERSION


def test_opensky_fields_match_poller():
    assert OPENSKY_FIELDS == opensky.FIELDS


def test_silver_key_matches_bronze_key():
    rec = opensky_record()
    assert rec.s3_key_silver == rec.s3_key_bronze.replace("bronze/", "silver/", 1)


# ============================================================== silver_handler
def put_bronze(s3, rec):
    s3.objects[(BUCKET, rec.s3_key_bronze)] = rec.to_json().encode()
    return rec.s3_key_bronze


def test_silver_handler_writes_silver_for_url_encoded_key(s3):
    rec = opensky_record()
    key = put_bronze(s3, rec)
    assert "=" in key  # URL-encoded as %3D in S3 event notifications

    out = silver_handler.lambda_handler(s3_event(key))

    assert out == {"written": 1, "skipped": 0, "stale": 0, "ignored": 0}
    assert s3.doc(rec.s3_key_silver) == to_silver(rec)


def test_silver_handler_skips_inactive_satellite(s3):
    rec = make_record("noaa_swpc", "noaa_swpc:wind:ACE:2026-11-20T11:55:00Z",
                      {"series": "wind", "source": "ACE", "active": False})
    out = silver_handler.lambda_handler(s3_event(put_bronze(s3, rec)))
    assert out == {"written": 0, "skipped": 1, "stale": 0, "ignored": 0}
    assert s3.keys("silver/") == []


def test_silver_handler_ignores_non_bronze_objects_without_reading(s3):
    out = silver_handler.lambda_handler(s3_event("silver/source=opensky/date=2026-11-20/x.json"))
    assert out == {"written": 0, "skipped": 0, "stale": 0, "ignored": 1}
    assert s3.reads == []  # prevents silver -> silver feedback loop


def test_silver_handler_processing_is_idempotent(s3):
    rec = opensky_record()
    key = put_bronze(s3, rec)
    silver_handler.lambda_handler(s3_event(key))
    first = s3.objects[(BUCKET, rec.s3_key_silver)]
    silver_handler.lambda_handler(s3_event(key))
    assert s3.objects[(BUCKET, rec.s3_key_silver)] == first
    assert len(s3.keys("silver/")) == 1


def test_silver_handler_corrupt_bronze_raises_exception(s3):
    key = "bronze/source=opensky/date=2026-11-20/opensky_bad_1.json"
    s3.objects[(BUCKET, key)] = b"{not json"
    with pytest.raises(Exception):
        silver_handler.lambda_handler(s3_event(key))


def seismic_version(lastupdate, mag):
    msg = copy.deepcopy(SEISMIC_MSG)
    msg["action"] = "update"
    msg["data"]["properties"].update(lastupdate=lastupdate, mag=mag)
    return seismic_record(msg)


def test_silver_seismic_latest_version_wins_regardless_of_order(s3):
    old = seismic_version("2026-11-16T19:29:23.1Z", 3.2)
    new = seismic_version("2026-11-16T19:29:23.9Z", 3.4)  # same second: sub-second precision matters

    assert silver_handler.lambda_handler(s3_event(put_bronze(s3, new)))["written"] == 1
    assert silver_handler.lambda_handler(s3_event(put_bronze(s3, old)))["stale"] == 1

    assert s3.keys("silver/") == [new.s3_key_silver]
    assert s3.doc(new.s3_key_silver)["data"]["mag"] == 3.4
    assert len(s3.keys("bronze/")) == 2  # bronze retains both versions


def test_silver_seismic_replay_of_same_version_overwrites(s3):
    rec = seismic_record()
    key = put_bronze(s3, rec)
    silver_handler.lambda_handler(s3_event(key))
    assert silver_handler.lambda_handler(s3_event(key))["written"] == 1


def test_silver_seismic_concurrent_write_does_not_overwrite_newer_version(s3, monkeypatch):
    old = seismic_version("2026-11-16T19:00:00Z", 3.2)
    new = seismic_version("2026-11-16T20:00:00Z", 3.4)
    put_bronze(s3, old)
    put_bronze(s3, new)
    silver_handler.lambda_handler(s3_event(old.s3_key_bronze))

    # Lambda A (old version) reads silver before Lambda B writes the newer version
    real_get = s3.get_object
    calls = {"n": 0}

    def get_then_race(Bucket, Key):
        resp = real_get(Bucket, Key)
        if Key == old.s3_key_silver and calls["n"] == 0:
            calls["n"] += 1
            silver_handler.process_object(BUCKET, new.s3_key_bronze)  # Lambda B wins the race
        return resp

    monkeypatch.setattr(s3, "get_object", get_then_race)
    assert silver_handler.process_object(BUCKET, old.s3_key_bronze) == "stale"
    assert s3.doc(new.s3_key_silver)["data"]["mag"] == 3.4


# ========================================= end-to-end: pollers -> bronze -> silver
def test_end_to_end_pollers_archiver_silver_flow(s3):
    wind_rows = [
        {"time_tag": "2026-11-20T11:57:00", "source": "ACE", "active": False, "proton_speed": None},
        {"time_tag": "2026-11-20T11:57:00", "source": "DSCOVR", "active": True, "proton_speed": 410.5},
    ]
    noaa = noaa_swpc.parse_rtsw(wind_rows, "wind", NOW, since="2026-11-20T11:00:00Z")
    noaa += noaa_swpc.parse_kp([{"time_tag": "2026-11-20T09:00:00", "Kp": 3.33, "a_running": 15, "station_count": 8}], NOW)
    flights = opensky.parse({"time": 1794824060, "states": [STATE, ["abc123", "TEST    "] + [None] * 16]}, NOW)
    weather = [tomorrow_io.parse({
        "data": {"time": "2026-11-20T11:55:00Z", "values": {"windSpeed": 5.2}},
        "location": {"lat": 52.1, "lon": 20.9},
    }, "EPWA", NOW)]
    update = copy.deepcopy(SEISMIC_MSG)
    update["action"] = "update"
    update["data"]["properties"].update(mag=3.4, lastupdate="2026-11-16T20:00:00.5Z")
    quakes = [seismic_record(), seismic_record(update)]  # two revisions of the same earthquake
    records = noaa + flights + weather + quakes

    # 1) SQS -> archiver -> bronze (everything, including inactive satellite and raw arrays)
    assert archiver.lambda_handler(sqs_event(records)) == {"batchItemFailures": []}
    bronze = s3.keys("bronze/")
    assert len(bronze) == len(records) == 8
    for rec in records:
        assert Record.from_json(s3.objects[(BUCKET, rec.s3_key_bronze)]) == rec  # payload identical to poller

    # 2) S3 events -> silver
    totals = {"written": 0, "skipped": 0, "stale": 0, "ignored": 0}
    for key in bronze:
        for k, v in silver_handler.lambda_handler(s3_event(key)).items():
            totals[k] += v
    assert totals == {"written": 7, "skipped": 1, "stale": 0, "ignored": 0}  # only inactive ACE satellite skipped

    silver = s3.keys("silver/")
    assert len(silver) == 6  # two seismic revisions => single silver object
    quake = s3.doc(quakes[1].s3_key_silver)
    assert quake["data"]["mag"] == 3.4 and quake["data"]["action"] == "update"
    # silver triggered no extra writes to bronze: bronze remains untouched
    assert s3.keys("bronze/") == bronze
    assert {s3.doc(k)["source"] for k in silver} == {"noaa_swpc", "opensky", "tomorrow_io", "seismic"}
