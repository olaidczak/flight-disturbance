import pytest

from common.schema import Record, make_event_id, to_utc_iso

T = "2026-11-16T10:20:00Z"


def test_roundtrip_and_keys():
    r = Record(make_event_id("opensky", "4952c7", 1), "opensky", T, T, {"lat": 1.0, "lon": 2.0}, {"a": 1})
    assert Record.from_json(r.to_json()) == r
    assert r.s3_key_bronze.startswith("bronze/source=opensky/date=2026-11-16/")
    assert len(r.deduplication_id) == 64  # limit SQS: 128 znaków


def test_time_formats():
    assert to_utc_iso(1794824057) == "2026-11-16T10:14:17Z"
    assert to_utc_iso("2026-11-16T10:20:00") == T  # bez strefy = UTC
    assert to_utc_iso("2026-11-16T11:20:00+01:00") == T
    assert to_utc_iso("2026-11-16T09:57:26.025781Z") == "2026-11-16T09:57:26Z"


@pytest.mark.parametrize("kwargs", [
    dict(event_id="seismic:1", source="opensky"),   # zły prefiks
    dict(event_id="x:1", source="nieznane"),        # nieznane źródło
])
def test_validation(kwargs):
    with pytest.raises(ValueError):
        Record(event_time=T, ingestion_time=T, location=None, **kwargs)
