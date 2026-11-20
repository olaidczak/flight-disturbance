from ingestion import opensky as o

NOW = "2026-11-16T10:25:00Z"

BODY = {
    "time": 1794824058,
    "states": [
        ["4952c7", "TAP1240 ", "Portugal", 1794824057, 1794824057, 14.4633, 49.9847, 2476.5, False,
         158.04, 38.92, -7.15, None, 2446.02, "6406", False, 0, 0],
        # na ziemi, brak wysokości, time_position = None
        ["488206", "SPARG   ", "Poland", None, 1794824028, 20.9799, 52.1592, None, True,
         4.63, 39.38, None, None, None, "1000", False, 0, 0],
        # brak pozycji
        ["abc123", "", "Poland", None, 1794824000, None, None, None, False,
         None, None, None, None, None, None, False, 0, 0],
    ],
}


def test_parse_fields_and_ids():
    recs = o.parse(BODY, NOW)
    assert len(recs) == 3
    a = recs[0]
    assert a.event_id == "opensky:4952c7:1794824057"
    assert a.event_time == "2026-11-16T10:14:17Z"
    assert a.location == {"lat": 49.9847, "lon": 14.4633}
    assert a.payload["callsign"] == "TAP1240"  # bez spacji
    assert a.payload["velocity"] == 158.04
    assert a.payload["category"] == 0
    assert "latitude" not in a.payload


def test_null_time_position_uses_last_contact():
    b = o.parse(BODY, NOW)[1]
    assert b.payload["time_position"] is None
    assert b.event_time == "2026-11-16T10:13:48Z"
    assert b.payload["baro_altitude"] is None and b.payload["on_ground"] is True


def test_missing_position_and_empty_callsign():
    c = o.parse(BODY, NOW)[2]
    assert c.location is None
    assert c.payload["callsign"] is None


def test_empty_response():
    assert o.parse({"time": 1, "states": None}, NOW) == []
    assert o.parse({"time": 1}, NOW) == []


class FakeResp:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def test_handler_rate_limited_does_not_raise(monkeypatch):
    monkeypatch.setattr(o, "fetch", lambda: FakeResp(429, headers={"X-Rate-Limit-Retry-After-Seconds": "3600"}))
    out = o.lambda_handler()
    assert out == {"sent": 0, "rate_limited": True, "retry_after": "3600"}


def test_handler_ok(monkeypatch):
    monkeypatch.setattr(o, "fetch", lambda: FakeResp(200, BODY, {"X-Rate-Limit-Remaining": "100"}))
    monkeypatch.setattr(o, "publish", lambda recs: len(recs))
    assert o.lambda_handler() == {"sent": 3, "rate_limited": False}
