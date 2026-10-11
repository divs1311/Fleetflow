import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

import pytest  # noqa: E402

from porterflow.config import load_settings  # noqa: E402


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("PORTERFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("SIM_TRIPS_PER_DAY", "1500")
    monkeypatch.setenv("PORTERFLOW_START_DATE", "2026-09-01")
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "")
    return load_settings()


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: end-to-end tests (dbt + Spark, slower)")
    config.addinivalue_line("markers", "kafka: needs a running Kafka/Redpanda at KAFKA_BOOTSTRAP_SERVERS")
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
