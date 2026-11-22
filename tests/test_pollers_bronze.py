"""Unit tests for ingestion pollers under Bronze layer requirements.

Verifies that:
- `payload` contains raw source data unchanged (no cleaning, renaming, or trimming),
- envelope complies with `common/schema.py` (`Record` passes validation),
- parsers do not mutate their input,
- `lambda_handler` passes those exact records to `publish()`.

Run: pytest -q tests/test_pollers_bronze.py
Tests do not call live external APIs or AWS: `fetch` and `publish` are mocked.
"""
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from common.schema import Record
from ingestion import noaa_swpc, opensky, tomorrow_io
from seismic_ws import main as seismic_ws

NOW = "2026-11-20T12:00:00Z"


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")  # matches NOAA time_tag format without 'Z'


class FakeResp:
    def __init__(self, body, status=200, headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


# ------------------------------------------------------------------ NOAA SWPC
def test_noaa_rtsw_payload_unchanged_without_satellite_filtering():
    rows = [
        {"time_tag": "2026-11-20T11:40:00", "source": "ACE", "active": True, "speed": 400.0},  # too old
        {"time_tag": "2026-11-20T11:57:00", "source": "ACE", "active": False, "speed": None},  # inactive + null
        {"time_tag": "2026-11-20T11:57:00", "source": "DSCOVR", "active": True, "speed": 410.5},
        {"time_tag": "2026-11-20T11:59:00", "source": "DSCOVR", "active": True, "speed": 411.0},
        {"source": "ACE"},  # missing time_tag: cannot build event_id
    ]
    original = copy.deepcopy(rows)

    recs = noaa_swpc.parse_rtsw(rows, "wind", NOW, since="2026-11-20T11:54:00Z")

    assert rows == original, "parser mutated input"
    assert len(recs) == 3
    for r in recs:
        assert isinstance(r, Record)
        assert r.payload["series"] == "wind"
        assert {k: v for k, v in r.payload.items() if k != "series"} in original
    # inactive satellite and null values are preserved (cleaning happens in Silver)
    assert any(r.payload["active"] is False and r.payload["speed"] is None for r in recs)
    # same time_tag across different satellites => distinct event_id, S3 keys, and deduplication_id
    assert len({r.event_id for r in recs}) == 3
    assert len({r.s3_key_bronze for r in recs}) == 3
    assert len({r.deduplication_id for r in recs}) == 3
    assert Record.from_json(recs[0].to_json()) == recs[0]


def test_noaa_kp_takes_latest_row_and_correction_creates_new_record():
    rows = [
        {"time_tag": "2026-11-20T03:00:00", "Kp": 2.0, "a_running": 7, "station_count": 8},
        {"time_tag": "2026-11-20T09:00:00", "Kp": 3.33, "a_running": 15, "station_count": 8},
    ]
    kp = noaa_swpc.parse_kp(rows, NOW)
    assert len(kp) == 1
    assert kp[0].event_id == "noaa_swpc:kp:2026-11-20T09:00:00Z:3.33"
    assert {k: v for k, v in kp[0].payload.items() if k != "series"} == rows[1]

    corrected = noaa_swpc.parse_kp([{**rows[1], "Kp": 3.67}], NOW)
    assert corrected[0].event_id != kp[0].event_id
    assert noaa_swpc.parse_kp([], NOW) == []


def test_noaa_handler_sends_raw_rows_in_window(monkeypatch):
    now = datetime.now(timezone.utc)
    fresh = _iso(now - timedelta(minutes=1))
    old = _iso(now - timedelta(minutes=noaa_swpc.WINDOW_MIN + 30))
    data = {
        "wind": [
            {"time_tag": old, "source": "ACE", "speed": 1.0},
            {"time_tag": fresh, "source": "ACE", "active": False, "speed": None},
            {"time_tag": fresh, "source": "DSCOVR", "active": True, "speed": 410.0},
        ],
        "mag": [{"time_tag": fresh, "source": "DSCOVR", "bt": 5.0}],
        "kp": [{"time_tag": "2026-11-20T09:00:00", "Kp": 3.33}],
    }
    by_url = {url: data[kind] for kind, url in noaa_swpc.URLS.items()}
    monkeypatch.setattr(noaa_swpc, "fetch", lambda url: copy.deepcopy(by_url[url]))
    sent = []
    monkeypatch.setattr(noaa_swpc, "publish", lambda recs: sent.extend(recs) or len(recs))

    out = noaa_swpc.lambda_handler()

    assert out == {"sent": 4, "errors": []}  # 2 wind + 1 mag + 1 kp; old row excluded
    assert sorted(r.payload["series"] for r in sent) == ["kp", "mag", "wind", "wind"]
    assert all(r.location is None for r in sent)


def test_noaa_handler_isolates_single_source_failure(monkeypatch):
    now = datetime.now(timezone.utc)
    fresh = _iso(now - timedelta(minutes=1))

    def fake_fetch(url):
        if url == noaa_swpc.URLS["mag"]:
            raise RuntimeError("boom")
        if url == noaa_swpc.URLS["kp"]:
            return [{"time_tag": "2026-11-20T09:00:00", "Kp": 2.0}]
        return [{"time_tag": fresh, "source": "DSCOVR", "speed": 400.0}]

    monkeypatch.setattr(noaa_swpc, "fetch", fake_fetch)
    monkeypatch.setattr(noaa_swpc, "publish", lambda recs: len(recs))

    out = noaa_swpc.lambda_handler()
    assert out["sent"] == 2
    assert len(out["errors"]) == 1 and out["errors"][0].startswith("mag:")


# -------------------------------------------------------------------- OpenSky
STATE_OK = [
    "4952c7", "LOT123  ", "Poland", 1794824050, 1794824057, 21.0, 52.2, 10000.0, False,
    230.0, 90.0, 0.0, None, 10100.0, "1000", False, 0, 3,
]
STATE_NO_POS = [
    "abc123", "TEST    ", "Poland", None, 1794824000, None, None, None, True,
    0.0, None, None, None, None, None, False, 0, 0,
]
STATE_NO_ID = [None, "X", "Poland", None, 1794824000] + [None] * 13


def test_opensky_payload_is_raw_state_array():
    body = {"time": 1794824060, "states": [STATE_OK, STATE_NO_POS, STATE_NO_ID]}
    original = copy.deepcopy(body)

    recs = opensky.parse(body, NOW)

    assert body == original, "parser mutated input"
    assert len(recs) == 2  # state without icao24 skipped
    first, second = recs
    assert first.payload == {"state": STATE_OK, "snapshot_time": 1794824060}
    assert first.payload["state"][1] == "LOT123  ", "callsign must not be trimmed in Bronze"
    assert first.event_id == "opensky:4952c7:1794824057"
    expected_time = datetime.fromtimestamp(1794824057, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert first.event_time == expected_time
    assert first.location == {"lat": 52.2, "lon": 21.0}
    assert second.location is None and second.payload["state"] == STATE_NO_POS


def test_opensky_null_last_contact_falls_back_to_snapshot_time():
    state = list(STATE_OK)
    state[4] = None
    recs = opensky.parse({"time": 1794824060, "states": [state]}, NOW)
    assert recs[0].event_id == "opensky:4952c7:1794824060"
    assert recs[0].payload["state"] == state


def test_opensky_empty_response():
    assert opensky.parse({"time": 1, "states": None}, NOW) == []
    assert opensky.parse({"time": 1}, NOW) == []


def test_opensky_fields_has_18_entries():
    assert len(opensky.FIELDS) == 18


def test_opensky_handler_counts_skipped_states(monkeypatch):
    body = {"time": 1794824060, "states": [STATE_OK, STATE_NO_POS, STATE_NO_ID]}
    monkeypatch.setattr(opensky, "fetch", lambda: FakeResp(body))
    sent = []
    monkeypatch.setattr(opensky, "publish", lambda recs: sent.extend(recs) or len(recs))

    out = opensky.lambda_handler()

    assert out == {"sent": 2, "skipped": 1, "rate_limited": False}
    assert sent[0].payload["state"] == STATE_OK


def test_opensky_handler_429_does_not_publish(monkeypatch):
    monkeypatch.setattr(
        opensky, "fetch", lambda: FakeResp({}, status=429, headers={"X-Rate-Limit-Retry-After-Seconds": "60"})
    )

    def no_publish(recs):
        raise AssertionError("publish must not be called on HTTP 429")

    monkeypatch.setattr(opensky, "publish", no_publish)

    out = opensky.lambda_handler()
    assert out["rate_limited"] is True and out["sent"] == 0 and out["retry_after"] == "60"


# ---------------------------------------------------------------- Tomorrow.io
def _tomorrow_body(lat=52.166, lon=20.967):
    return {
        "data": {
            "time": "2026-11-20T11:55:00Z",
            "values": {"temperature": 3.1, "windSpeed": 5.2, "visibility": 9.8},
        },
        "location": {"lat": lat, "lon": lon, "name": "Warsaw", "type": "administrative"},
    }


def test_tomorrow_payload_contains_full_response():
    body = _tomorrow_body()
    original = copy.deepcopy(body)

    rec = tomorrow_io.parse(body, "EPWA", NOW)

    assert body == original, "parser mutated input"
    assert rec.payload == {"station": "EPWA", "response": original}
    assert rec.event_id == "tomorrow_io:EPWA:2026-11-20T11:55:00Z"
    assert rec.event_time == "2026-11-20T11:55:00Z"
    assert rec.location == {"lat": 52.166, "lon": 20.967}
    assert rec.s3_key_bronze == (
        "bronze/source=tomorrow_io/date=2026-11-20/tomorrow_io_EPWA_2026-11-20T11_55_00Z.json"
    )


def test_tomorrow_handler_isolates_single_airport_429(monkeypatch):
    monkeypatch.setattr(tomorrow_io, "PAUSE_S", 0)
    monkeypatch.setattr(tomorrow_io, "tomorrow_api_keys", lambda: ["k1", "k2"])
    epkk_lat = tomorrow_io.LOCATIONS["EPKK"][0]

    def fake_fetch(lat, lon, key):
        if lat == epkk_lat:
            return FakeResp({}, status=429)
        return FakeResp(_tomorrow_body(lat, lon))

    monkeypatch.setattr(tomorrow_io, "fetch", fake_fetch)
    sent = []
    monkeypatch.setattr(tomorrow_io, "publish", lambda recs: sent.extend(recs) or len(recs))

    out = tomorrow_io.lambda_handler()

    n = len(tomorrow_io.LOCATIONS)
    assert out["sent"] == n - 1
    assert out["errors"] == ["EPKK: 429"]
    assert {r.payload["station"] for r in sent} == set(tomorrow_io.LOCATIONS) - {"EPKK"}
    assert all("response" in r.payload and "values" not in r.payload for r in sent)


def test_tomorrow_handler_raises_when_all_airports_fail(monkeypatch):
    monkeypatch.setattr(tomorrow_io, "PAUSE_S", 0)
    monkeypatch.setattr(tomorrow_io, "tomorrow_api_keys", lambda: ["k1"])
    monkeypatch.setattr(tomorrow_io, "fetch", lambda lat, lon, key: FakeResp({}, status=500))
    monkeypatch.setattr(tomorrow_io, "publish", lambda recs: len(recs))
    with pytest.raises(RuntimeError):
        tomorrow_io.lambda_handler()


# ------------------------------------------------------------------ seismic (EMSC WebSocket)
SEISMIC_MSG = json.loads((Path(__file__).parent / "seismic_sample.json").read_text())


def test_seismic_payload_contains_full_message_and_envelope():
    msg = copy.deepcopy(SEISMIC_MSG)
    rec = seismic_ws.parse(msg, NOW)

    assert msg == SEISMIC_MSG, "parser must not mutate input"
    assert rec.payload == SEISMIC_MSG
    assert rec.event_id == "seismic:20261115_0000312:2026-11-16T19:29:23.33162Z"
    assert rec.event_time == "2026-11-15T05:08:52Z"
    assert rec.location == {"lat": 7.5292, "lon": -80.4598, "depth": 15.0}  # positive depth (km)
    assert Record.from_json(rec.to_json()) == rec


def test_seismic_update_creates_new_record_while_duplicate_matches():
    update = copy.deepcopy(SEISMIC_MSG)
    update["action"] = "update"
    update["data"]["properties"]["lastupdate"] = "2026-11-16T20:00:00Z"

    first = seismic_ws.parse(SEISMIC_MSG, NOW)
    again = seismic_ws.parse(SEISMIC_MSG, "2026-11-20T12:05:00Z")
    updated = seismic_ws.parse(update, NOW)

    assert again.deduplication_id == first.deduplication_id
    assert updated.deduplication_id != first.deduplication_id
    assert updated.s3_key_bronze != first.s3_key_bronze


def test_seismic_message_missing_envelope_fields_raises():
    with pytest.raises(KeyError):
        seismic_ws.parse({"action": "create", "data": {"properties": {"unid": "x"}}}, NOW)
