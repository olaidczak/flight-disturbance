"""Testy warstwy lake: archiver (bronze), silver_transforms, silver_handler
oraz przepływ end-to-end: pollery -> archiver -> silver (na atrapie S3).

Uruchomienie z korzenia repo (dopasuj ścieżki do swojej struktury):
    PYTHONPATH=src:src/ingestion pytest -q tests/test_lake.py
Testy nie wołają AWS ani prawdziwych API.
"""
import copy
import dataclasses
import importlib
import io
import json
from datetime import datetime, timezone
from urllib.parse import quote_plus

import pytest

from common.schema import Record
from processing import archiver, silver_handler, silver_transforms
from processing.silver_transforms import OPENSKY_FIELDS, to_silver



def _load_poller(name):
    """Znajduje moduł pollera niezależnie od układu katalogów (src/ingestion, src/ingestion/pollers
    albo płasko na PYTHONPATH). Brakującą zależność w środku modułu (np. requests) zgłasza normalnie."""
    tried = []
    for pkg in ("ingestion", "ingestion.pollers", "pollers", ""):
        mod = f"{pkg}.{name}" if pkg else name
        tried.append(mod)
        try:
            return importlib.import_module(mod)
        except ModuleNotFoundError as e:
            if e.name and (mod == e.name or mod.startswith(e.name + ".")):
                continue  # to nie ten układ katalogów
            raise
    raise ImportError(f"Nie znaleziono pollera '{name}'. Próbowano: {', '.join(tried)}")


noaa_swpc = _load_poller("noaa_swpc")
opensky = _load_poller("opensky")
tomorrow_io = _load_poller("tomorrow_io")

BUCKET = "lake-test"
NOW = "2026-11-20T12:00:00Z"


# ------------------------------------------------------------------- atrapa S3
class PreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class AccessDenied(Exception):
    response = {"Error": {"Code": "AccessDenied"}}


class FakeS3:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.reads: list[str] = []
        self.fail_with: Exception | None = None

    def put_object(self, Bucket, Key, Body, ContentType=None, IfNoneMatch=None):
        if self.fail_with:
            raise self.fail_with
        if IfNoneMatch == "*" and (Bucket, Key) in self.objects:
            raise PreconditionFailed()
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

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


# ------------------------------------------------------------------ pomocnicze
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


def opensky_record(state=None, snapshot=1794824060):
    body = {"time": snapshot, "states": [state or STATE]}
    return opensky.parse(body, NOW)[0]


# =========================================================== archiver (bronze)
def test_archiver_zapisuje_rekord_bez_zmian_pod_kluczem_bronze(s3):
    rec = opensky_record()
    out = archiver.lambda_handler(sqs_event([rec]))

    assert out == {"batchItemFailures": []}
    assert s3.keys() == [rec.s3_key_bronze]
    assert s3.keys()[0].startswith("bronze/source=opensky/date=")
    stored = Record.from_json(s3.objects[(BUCKET, rec.s3_key_bronze)])
    assert stored == rec
    assert stored.payload["state"][1] == "LOT123  "  # callsign nietknięty


def test_archiver_duplikat_pierwszy_zapis_wygrywa(s3):
    first = opensky_record()
    second = dataclasses.replace(first, ingestion_time="2026-11-20T12:05:00Z")

    assert archiver.lambda_handler(sqs_event([first])) == {"batchItemFailures": []}
    assert archiver.lambda_handler(sqs_event([second])) == {"batchItemFailures": []}  # 412 = sukces

    assert len(s3.keys()) == 1
    assert s3.doc(first.s3_key_bronze)["ingestion_time"] == NOW  # zostaje czas pierwszego pobrania


def test_archiver_write_once_wylaczony_nadpisuje(s3, monkeypatch):
    monkeypatch.setenv("BRONZE_WRITE_ONCE", "false")
    first = opensky_record()
    second = dataclasses.replace(first, ingestion_time="2026-11-20T12:05:00Z")

    archiver.lambda_handler(sqs_event([first]))
    archiver.lambda_handler(sqs_event([second]))

    assert s3.doc(first.s3_key_bronze)["ingestion_time"] == "2026-11-20T12:05:00Z"


def test_archiver_blad_wiadomosci_blokuje_ta_sama_grupe_ale_nie_inne(s3):
    ok1 = opensky_record()
    bad = opensky_record()
    later_same_group = opensky_record(state=["abc123"] + STATE[1:])
    other_group = noaa_swpc.parse_kp([{"time_tag": "2026-11-20T09:00:00", "Kp": 3.0}], NOW)[0]

    event = sqs_event([ok1, bad, later_same_group, other_group], bodies=[None, "to nie jest json", None, None])
    out = archiver.lambda_handler(event)

    assert out["batchItemFailures"] == [{"itemIdentifier": "m1"}, {"itemIdentifier": "m2"}]
    assert ok1.s3_key_bronze in s3.keys()
    assert other_group.s3_key_bronze in s3.keys()
    assert later_same_group.s3_key_bronze not in s3.keys()  # kolejność w grupie zachowana


def test_archiver_inny_blad_s3_jest_zglaszany(s3):
    s3.fail_with = AccessDenied()
    out = archiver.lambda_handler(sqs_event([opensky_record()]))
    assert out["batchItemFailures"] == [{"itemIdentifier": "m0"}]


def test_archiver_odrzuca_zla_koperte(s3):
    bad = json.dumps({"event_id": "zle:id", "source": "opensky", "event_time": NOW,
                      "ingestion_time": NOW, "location": None, "payload": {}})
    out = archiver.lambda_handler(sqs_event([opensky_record()], bodies=[bad]))
    assert out["batchItemFailures"] == [{"itemIdentifier": "m0"}]
    assert s3.keys() == []


# ======================================================== silver_transforms
def test_noaa_wind_aktywny_satelita_jest_czyszczony():
    rec = make_record("noaa_swpc", "noaa_swpc:wind:DSCOVR:2026-11-20T11:55:00Z", {
        "series": "wind", "time_tag": "2026-11-20T11:55:00", "source": "DSCOVR", "active": True,
        "proton_speed": "410.5", "proton_density": None, "range": 1,
    })
    doc = to_silver(rec)
    assert doc["data"] == {"series": "wind", "satellite": "DSCOVR",
                           "proton_speed": 410.5, "proton_density": None, "range": 1}


def test_noaa_nieaktywny_satelita_jest_pomijany_a_brak_pola_active_nie():
    base = {"series": "mag", "time_tag": "2026-11-20T11:55:00", "source": "ACE", "bt": 5.0}
    inactive = make_record("noaa_swpc", "noaa_swpc:mag:ACE:t", {**base, "active": False})
    unknown = make_record("noaa_swpc", "noaa_swpc:mag:ACE:t2", dict(base))
    assert to_silver(inactive) is None
    assert to_silver(unknown)["data"]["satellite"] == "ACE"


def test_noaa_kp():
    rec = make_record("noaa_swpc", "noaa_swpc:kp:2026-11-20T09:00:00Z:3.33", {
        "series": "kp", "time_tag": "2026-11-20T09:00:00", "Kp": "3.33", "a_running": 15, "station_count": 8,
    })
    assert to_silver(rec)["data"] == {"series": "kp", "kp": 3.33, "a_running": 15, "station_count": 8}


def test_opensky_nazwane_pola_callsign_i_czasy():
    rec = opensky_record()
    original = copy.deepcopy(rec.payload)
    doc = to_silver(rec)
    d = doc["data"]

    assert rec.payload == original, "silver nie może zmieniać rekordu bronze"
    assert d["icao24"] == "4952c7"
    assert d["callsign"] == "LOT123"
    iso = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
    assert d["last_contact"] == iso(1794824057)
    assert d["time_position"] == iso(1794824050)
    assert d["snapshot_time"] == iso(1794824060)
    assert d["on_ground"] is False and d["velocity"] == 230.0 and d["baro_altitude"] == 10000.0
    assert "sensors" not in d
    assert doc["location"] == {"lat": 52.2, "lon": 21.0}


def test_opensky_pusty_callsign_none_i_krotka_tablica():
    state = list(STATE)
    state[1] = "        "
    d = to_silver(opensky_record(state=state))["data"]
    assert d["callsign"] is None

    short = to_silver(make_record("opensky", "opensky:z:1", {"state": ["z", None, "Poland", None, 1794824057], "snapshot_time": None}))
    assert short["data"]["velocity"] is None and short["data"]["snapshot_time"] is None


def test_tomorrow_splaszczenie_i_snake_case():
    resp = {
        "data": {"time": "2026-11-20T11:55:00Z",
                 "values": {"temperature": 3.1, "windSpeed": 5.2, "uvIndex": 0, "temperatureApparent": 1.0}},
        "location": {"lat": 52.166, "lon": 20.967, "name": "Warsaw"},
    }
    rec = tomorrow_io.parse(resp, "EPWA", NOW)
    d = to_silver(rec)["data"]
    assert d == {"station": "EPWA", "temperature": 3.1, "wind_speed": 5.2, "uv_index": 0, "temperature_apparent": 1.0}


def test_nieznane_zrodlo_przechodzi_bez_zmian():
    rec = make_record("seismic", "seismic:20261120_0000001:2026-11-20T11:55:00Z", {"mag": 4.1, "region": "X"})
    assert to_silver(rec)["data"] == {"mag": 4.1, "region": "X"}


def test_koperta_silver():
    rec = opensky_record()
    doc = to_silver(rec)
    for k in ("event_id", "source", "event_time", "ingestion_time", "location"):
        assert doc[k] == getattr(rec, k)
    assert doc["silver_version"] == silver_transforms.SILVER_VERSION


def test_opensky_fields_zgodne_z_pollerem():
    assert OPENSKY_FIELDS == opensky.FIELDS


def test_klucz_silver_odpowiada_kluczowi_bronze():
    rec = opensky_record()
    assert rec.s3_key_silver == rec.s3_key_bronze.replace("bronze/", "silver/", 1)


# ============================================================== silver_handler
def put_bronze(s3, rec):
    s3.objects[(BUCKET, rec.s3_key_bronze)] = rec.to_json().encode()
    return rec.s3_key_bronze


def test_silver_handler_zapisuje_silver_dla_zakodowanego_klucza(s3):
    rec = opensky_record()
    key = put_bronze(s3, rec)
    assert "=" in key  # w zdarzeniu S3 będzie zakodowane jako %3D

    out = silver_handler.lambda_handler(s3_event(key))

    assert out == {"written": 1, "skipped": 0, "ignored": 0}
    assert s3.doc(rec.s3_key_silver) == to_silver(rec)


def test_silver_handler_pomija_nieaktywnego_satelite(s3):
    rec = make_record("noaa_swpc", "noaa_swpc:wind:ACE:2026-11-20T11:55:00Z",
                      {"series": "wind", "source": "ACE", "active": False})
    out = silver_handler.lambda_handler(s3_event(put_bronze(s3, rec)))
    assert out == {"written": 0, "skipped": 1, "ignored": 0}
    assert s3.keys("silver/") == []


def test_silver_handler_ignoruje_obiekty_spoza_bronze_bez_czytania(s3):
    out = silver_handler.lambda_handler(s3_event("silver/source=opensky/date=2026-11-20/x.json"))
    assert out == {"written": 0, "skipped": 0, "ignored": 1}
    assert s3.reads == []  # żadnej pętli silver -> silver


def test_silver_handler_przetwarzanie_jest_idempotentne(s3):
    rec = opensky_record()
    key = put_bronze(s3, rec)
    silver_handler.lambda_handler(s3_event(key))
    first = s3.objects[(BUCKET, rec.s3_key_silver)]
    silver_handler.lambda_handler(s3_event(key))
    assert s3.objects[(BUCKET, rec.s3_key_silver)] == first
    assert len(s3.keys("silver/")) == 1


def test_silver_handler_uszkodzony_bronze_rzuca_wyjatek(s3):
    key = "bronze/source=opensky/date=2026-11-20/opensky_bad_1.json"
    s3.objects[(BUCKET, key)] = b"{nie json"
    with pytest.raises(Exception):
        silver_handler.lambda_handler(s3_event(key))


# ========================================= end-to-end: pollery -> bronze -> silver
def test_przeplyw_pollery_archiver_silver(s3):
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
    records = noaa + flights + weather

    # 1) SQS -> archiver -> bronze (wszystko, także nieaktywny satelita i surowe tablice)
    assert archiver.lambda_handler(sqs_event(records)) == {"batchItemFailures": []}
    bronze = s3.keys("bronze/")
    assert len(bronze) == len(records) == 6
    for rec in records:
        assert Record.from_json(s3.objects[(BUCKET, rec.s3_key_bronze)]) == rec  # payload identyczny z pollerem

    # 2) zdarzenia S3 -> silver
    totals = {"written": 0, "skipped": 0, "ignored": 0}
    for key in bronze:
        for k, v in silver_handler.lambda_handler(s3_event(key)).items():
            totals[k] += v
    assert totals == {"written": 5, "skipped": 1, "ignored": 0}  # pominięty tylko nieaktywny satelita ACE

    silver = s3.keys("silver/")
    assert len(silver) == 5
    # silver nie wywołał kolejnych zdarzeń: bronze nietknięte
    assert s3.keys("bronze/") == bronze
    assert {s3.doc(k)["source"] for k in silver} == {"noaa_swpc", "opensky", "tomorrow_io"}
