from ingestion import noaa_swpc as n

NOW = "2026-11-16T10:25:00Z"
SINCE = "2026-11-16T10:00:00Z"

WIND = [
    {"time_tag": "2026-11-16T10:20:00", "active": True, "source": "SOLAR1", "proton_speed": 329.28, "proton_density": 1.09},
    {"time_tag": "2026-11-16T10:20:00", "active": False, "source": "ACE", "proton_speed": None, "proton_density": None},
    {"time_tag": "2026-11-15T10:22:00", "active": True, "source": "SOLAR1", "proton_speed": 368.8, "proton_density": 0.94},  # za stary
]
KP = [
    {"time_tag": "2026-11-16T03:00:00", "Kp": 1.0, "a_running": 4, "station_count": 8},
    {"time_tag": "2026-11-16T06:00:00", "Kp": 0.33, "a_running": 2, "station_count": 8},
]


def test_wind_filters_inactive_and_old():
    recs = n.parse_rtsw(WIND, "wind", NOW, SINCE)
    assert len(recs) == 1
    r = recs[0]
    assert r.event_id == "noaa_swpc:wind:2026-11-16T10:20:00Z"
    assert r.event_time == "2026-11-16T10:20:00Z"  # dopisane UTC
    assert r.location is None
    assert r.payload["proton_speed"] == 329.28 and r.payload["series"] == "wind"
    assert "active" not in r.payload and "time_tag" not in r.payload


def test_kp_takes_latest_only_and_value_in_id():
    recs = n.parse_kp(KP, NOW)
    assert len(recs) == 1
    assert recs[0].event_time == "2026-11-16T06:00:00Z"
    assert recs[0].event_id.endswith(":0.33")
    assert recs[0].payload["Kp"] == 0.33


def test_kp_empty():
    assert n.parse_kp([], NOW) == []


def test_handler_survives_one_failing_source(monkeypatch):
    def fake_fetch(url):
        if "kp" in url or "planetary" in url:
            raise RuntimeError("boom")
        return WIND

    sent = {}
    monkeypatch.setattr(n, "fetch", fake_fetch)
    monkeypatch.setattr(n, "publish", lambda recs: sent.setdefault("n", len(recs)))
    monkeypatch.setattr(n, "WINDOW_MIN", 10**7)  # okno obejmuje całą próbkę, także stary wiersz
    out = n.lambda_handler()
    assert out["sent"] == 4  # wind i mag: po 2 aktywne wiersze (oba dostają WIND), kp padło
    assert len(out["errors"]) == 1 and out["errors"][0].startswith("kp")
