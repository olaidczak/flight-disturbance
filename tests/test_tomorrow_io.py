from ingestion import tomorrow_io as t

NOW = "2026-11-16T10:25:00Z"

BODY = {
    "data": {
        "time": "2026-11-16T10:20:00Z",
        "values": {"temperature": 5.69, "windSpeed": 2.5, "windGust": 5.2, "visibility": 10,
                   "cloudCeiling": 10.7, "precipitationProbability": 0},
    },
    "location": {"lat": 52.17, "lon": 20.97},
}


def test_parse():
    r = t.parse(BODY, "EPWA", NOW)
    assert r.source == "tomorrow_io"
    assert r.event_id == "tomorrow_io:EPWA:2026-11-16T10:20:00Z"
    assert r.event_time == "2026-11-16T10:20:00Z"
    assert r.location == {"lat": 52.17, "lon": 20.97}
    assert r.payload["station"] == "EPWA" and r.payload["windGust"] == 5.2


class FakeResp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def test_handler_spreads_keys_and_survives_429(monkeypatch):
    used = []

    def fake_fetch(lat, lon, key):
        used.append(key)
        return FakeResp(429) if len(used) == 2 else FakeResp(200, BODY)

    monkeypatch.setattr(t, "fetch", fake_fetch)
    monkeypatch.setattr(t, "tomorrow_api_keys", lambda: ["K1", "K2"])
    monkeypatch.setattr(t, "publish", lambda recs: len(recs))
    monkeypatch.setattr(t, "PAUSE_S", 0)
    out = t.lambda_handler()
    assert used == ["K1", "K2", "K1", "K2", "K1"]  # klucze naprzemiennie
    assert out["sent"] == len(t.LOCATIONS) - 1
    assert len(out["errors"]) == 1


def test_handler_raises_when_everything_fails(monkeypatch):
    import pytest

    monkeypatch.setattr(t, "fetch", lambda *a: FakeResp(429))
    monkeypatch.setattr(t, "tomorrow_api_keys", lambda: ["K1"])
    monkeypatch.setattr(t, "publish", lambda recs: len(recs))
    monkeypatch.setattr(t, "PAUSE_S", 0)
    with pytest.raises(RuntimeError):
        t.lambda_handler()
