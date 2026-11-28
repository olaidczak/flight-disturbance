"""Unit tests for the Glue job code without Spark: batch_rows (parse/to_doc), batch_schemas, catch-up days.

Run from repository root:
    PYTHONPATH=src pytest -q tests/test_batch.py
Tests do not call AWS or live external APIs.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from processing import batch_rows, batch_schemas
from processing.silver_transforms import to_silver
from test_lake import NOW, noaa_swpc, opensky_record, seismic_record, tomorrow_io

HERE = Path(__file__).parent
NOAA_SAMPLE = json.loads((HERE / "noaa_sample.json").read_text())
TOMORROW_SAMPLE = json.loads((HERE / "tomorrow_io_sample.json").read_text())
SINCE = "2026-11-01T00:00:00Z"


def sample_records():
    """One or more sample records for every silver dataset (from the *_sample.json files)."""
    wind = noaa_swpc.parse_rtsw([r for r in NOAA_SAMPLE["wind"] if r["active"]], "wind", NOW, since=SINCE)
    mag = noaa_swpc.parse_rtsw([r for r in NOAA_SAMPLE["mag"] if r["active"]], "mag", NOW, since=SINCE)
    kp = noaa_swpc.parse_kp(NOAA_SAMPLE["kp"], NOW)
    weather = tomorrow_io.parse(
        {"data": {"time": "2026-11-20T11:55:00Z", "values": TOMORROW_SAMPLE["data"]["values"]},
         "location": {"lat": 52.1, "lon": 20.9}}, "EPWA", NOW)
    return {
        "opensky": [opensky_record()],
        "tomorrow_io": [weather],
        "seismic": [seismic_record()],
        "noaa_swpc_wind": wind,
        "noaa_swpc_mag": mag,
        "noaa_swpc_kp": kp,
    }


# ------------------------------------------------------------------ schemas
@pytest.mark.parametrize("name", [d[0] for d in batch_schemas.DATASETS])
def test_schema_covers_every_silver_field_from_samples(name):
    records = sample_records()[name]
    assert records, f"no sample records for {name}"
    lines = [json.dumps(to_silver(r)) for r in records]
    assert batch_schemas.unknown_fields(name, lines) == set()


def test_schema_detects_new_field():
    doc = to_silver(opensky_record())
    doc["data"]["new_field"] = 1
    assert batch_schemas.unknown_fields("opensky", [json.dumps(doc)]) == {"new_field"}


@pytest.mark.parametrize("name", [d[0] for d in batch_schemas.DATASETS])
def test_ddl_has_envelope_and_all_data_fields(name):
    ddl = batch_schemas.ddl(name)
    for col in ("event_id STRING", "ingestion_time STRING", "location STRUCT<lat: DOUBLE, lon: DOUBLE, depth: DOUBLE>",
                "silver_version INT", "ingest_date STRING"):
        assert col in ddl
    for field in batch_schemas.data_fields(name):
        assert f"`{field}`:" in ddl
    assert ddl.count("<") == ddl.count(">")
    assert len(batch_schemas.data_fields(name)) == len(set(batch_schemas.data_fields(name)))  # no duplicates


def test_dataset_name_and_datasets_cover_all_sources():
    assert batch_schemas.dataset_name("noaa_swpc", "wind") == "noaa_swpc_wind"
    assert batch_schemas.dataset_name("opensky") == "opensky"
    assert {s for _, s, _ in batch_schemas.DATASETS} == {"opensky", "tomorrow_io", "seismic", "noaa_swpc"}


# --------------------------------------------------------------- parse / to_doc
def row_from(rec, ingest_date="2026-11-20"):
    return SimpleNamespace(**dict(zip(batch_rows.COLS, batch_rows.parse(rec.to_json()))), ingest_date=ingest_date)


def test_parse_keeps_envelope_and_stores_location_and_payload_as_json_text():
    rec = opensky_record()
    event_id, source, event_time, ingestion_time, location, payload = batch_rows.parse(rec.to_json())
    assert (event_id, source, event_time, ingestion_time) == (rec.event_id, rec.source, rec.event_time, rec.ingestion_time)
    assert json.loads(location) == rec.location
    assert json.loads(payload) == rec.payload


def test_to_doc_equals_to_silver_plus_ingest_date():
    rec = opensky_record()
    doc = json.loads(batch_rows.to_doc(row_from(rec)))
    expected = to_silver(rec)
    expected["ingest_date"] = "2026-11-20"
    assert doc == expected
    assert doc["data"]["callsign"] == "LOT123"


def test_to_doc_inactive_satellite_is_none():
    inactive = noaa_swpc.parse_rtsw(
        [{"time_tag": "2026-11-20T11:57:00", "source": "ACE", "active": False, "proton_speed": 1.0}],
        "wind", NOW, since="2026-11-20T11:00:00Z")[0]
    assert batch_rows.to_doc(row_from(inactive)) is None


def test_seismic_silver_event_id_has_no_lastupdate():
    doc = json.loads(batch_rows.to_doc(row_from(seismic_record())))
    assert doc["event_id"] == seismic_record().silver_event_id
    assert doc["event_id"].count(":") == 1


# ------------------------------------------------------------------ catch-up days
def test_pending_days_skips_today_and_marked_days():
    raw = ["2026-11-25", "2026-11-26", "2026-11-27", "2026-11-28"]
    assert batch_rows.pending_days(raw, ["2026-11-26"], "2026-11-28") == ["2026-11-25", "2026-11-27"]


def test_pending_days_empty_when_everything_is_done():
    assert batch_rows.pending_days(["2026-11-27"], ["2026-11-27"], "2026-11-28") == []
