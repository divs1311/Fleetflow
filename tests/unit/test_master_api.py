from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from porterflow.api import app as api_module

HEADERS = {"X-API-Key": api_module.API_KEY}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api_module, "FAILURE_RATE", 0.0)
    return TestClient(api_module.app)


def test_requires_api_key(client):
    assert client.get("/v1/drivers", params={"updated_until": "2026-09-02T00:00:00Z"}).status_code == 401


def test_pagination_covers_every_record_exactly_once(client):
    params = {"updated_until": "2026-09-01T18:30:00Z", "page_size": 1000}
    first = client.get("/v1/drivers", params=params, headers=HEADERS).json()
    ids, page = [], 1
    while page:
        body = client.get("/v1/drivers", params={**params, "page": page}, headers=HEADERS).json()
        ids += [d["driver_id"] for d in body["data"]]
        page = body["next_page"]
    assert len(ids) == first["total_records"] == len(set(ids))


def test_incremental_window_returns_only_changes(client):
    window = {"updated_since": "2026-09-02T18:30:00Z", "updated_until": "2026-09-03T18:30:00Z", "page_size": 1000}
    body = client.get("/v1/drivers", params=window, headers=HEADERS).json()
    assert 0 < body["total_records"] < 200
    for d in body["data"]:
        ts = datetime.fromisoformat(d["updated_at_utc"].replace("Z", ""))
        assert datetime(2026, 9, 2, 18, 30) <= ts < datetime(2026, 9, 3, 18, 30)
    again = client.get("/v1/drivers", params=window, headers=HEADERS).json()
    assert again["data"] == body["data"]  # deterministic: backfills see the same answer


def test_rejects_inverted_window(client):
    r = client.get("/v1/vehicles", params={"updated_since": "2026-09-03T00:00:00Z",
                                           "updated_until": "2026-09-02T00:00:00Z"}, headers=HEADERS)
    assert r.status_code == 422


def test_flaky_api_is_flaky(monkeypatch):
    monkeypatch.setattr(api_module, "FAILURE_RATE", 1.0)
    r = TestClient(api_module.app).get("/v1/drivers", params={"updated_until": "2026-09-02T00:00:00Z"},
                                       headers=HEADERS)
    assert r.status_code in (429, 503)
