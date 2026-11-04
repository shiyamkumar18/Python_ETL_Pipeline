"""Smoke tests for the local mock API source."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from scripts.mock_event_api import app


@pytest.mark.unit
def test_mock_api_health_and_pagination(monkeypatch):
    monkeypatch.setenv("MOCK_API_FLAKE_RATE", "0")
    with TestClient(app) as client:
        health = client.get("/health")
        first = client.get("/events", params={"offset": 0, "limit": 2})
        second = client.get("/events", params={"offset": 2, "limit": 2})

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert first.status_code == second.status_code == 200
    assert first.json()["total"] == 5000
    assert len(first.json()["data"]) == 2
    assert first.json()["next_offset"] == 2
    assert first.json()["has_more"] is True
    assert second.json()["offset"] == 2
    assert first.json()["data"][0]["event_id"] != second.json()["data"][0]["event_id"]
