"""Shared fixtures for isolated ETL unit tests."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import SecretStr

from etl.config import Settings


@pytest.fixture
def make_event():
    """Build a valid raw event dictionary with optional overrides."""
    def _make(**overrides):
        base = {
            "event_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "event_name": "login",
            "occurred_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "session_id": str(uuid.uuid4()),
            "properties": {"ip": "192.0.2.10"},
        }
        base.update(overrides)
        return base
    return _make


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Create isolated settings without mutating process-wide environment."""
    return Settings(
        source_api_base_url="http://mock-api:8000",
        source_api_timeout_sec=1.0,
        source_api_max_retries=2,
        source_api_backoff_sec=0.001,
        source_batch_size=2,
        database_url=SecretStr("postgresql://test:test@localhost:5432/test"),
        db_insert_batch_size=2,
        reject_event_age_days=30,
        reject_rate_threshold_pct=100.0,
        max_properties_keys=10,
        max_properties_bytes=1024,
        data_dir=tmp_path,
        quarantine_dir=tmp_path / "quarantine",
        metrics_dir=tmp_path / "metrics",
        log_level="WARNING",
    )


@pytest.fixture
def transformer(settings):
    from etl.transform import Transformer
    return Transformer(settings, run_id="test-run")
