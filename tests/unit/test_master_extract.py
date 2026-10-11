from datetime import date

import pyarrow.parquet as pq
import pytest
import requests

from api_fakes import FlakyThenOK
from porterflow.ingestion import master_data


@pytest.fixture
def fake_session(monkeypatch):
    sessions = []

    def factory():
        s = FlakyThenOK()
        sessions.append(s)
        return s

    monkeypatch.setattr(master_data.requests, "Session", factory)
    monkeypatch.setattr(master_data.time, "sleep", lambda s: None)
    return sessions


def test_full_then_incremental_extract_is_idempotent(settings, fake_session):
    first = master_data.extract_master_data(settings, date(2026, 9, 1))
    assert first["resources"]["drivers"]["full_load"] is True
    second = master_data.extract_master_data(settings, date(2026, 9, 2))
    assert second["resources"]["drivers"]["full_load"] is False
    assert second["resources"]["drivers"]["rows"] < first["resources"]["drivers"]["rows"]
    again = master_data.extract_master_data(settings, date(2026, 9, 2))
    assert again["resources"] == second["resources"]
    path = settings.master_dir / "drivers" / "extract_date=2026-09-02" / "part-0.parquet"
    assert pq.read_table(path).num_rows == second["resources"]["drivers"]["rows"]
    assert fake_session[0].calls > 2  # the first two attempts failed and were retried


def test_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr(master_data.time, "sleep", lambda s: None)
    s = FlakyThenOK(fail_times=100)
    with pytest.raises(requests.HTTPError):
        master_data.get_with_retries(s, "http://api/v1/drivers", {}, max_attempts=3)
    assert s.calls == 3
