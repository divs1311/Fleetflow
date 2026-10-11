"""A fake requests.Session that fails a few times, then answers through the real FastAPI app."""
from __future__ import annotations

import requests
from fastapi.testclient import TestClient

from porterflow.api import app as api_module


class FlakyThenOK:
    """Fake requests.Session: fails `fail_times` times with 503/429, then answers via the real API app."""

    def __init__(self, fail_times=2, api_failure_rate=0.0):
        self.calls = 0
        self.fail_times = fail_times
        self.headers = {}
        api_module.FAILURE_RATE = api_failure_rate
        self.client = TestClient(api_module.app)

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        if self.calls <= self.fail_times:
            r = requests.Response()
            r.status_code = 503 if self.calls % 2 else 429
            r.headers["Retry-After"] = "0"
            return r
        path = url.split("://", 1)[1].split("/", 1)[1]
        resp = self.client.get("/" + path, params=params, headers=self.headers)
        r = requests.Response()
        r.status_code, r._content = resp.status_code, resp.content
        return r
