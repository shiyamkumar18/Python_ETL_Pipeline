"""Optional real-PostgreSQL integration test.

Set ETL_TEST_DATABASE_URL to a disposable PostgreSQL database to enable it.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest
from pydantic import SecretStr

from etl.config import Settings
from etl.load import Loader
from etl.transform import EventRow

TEST_DSN = os.getenv("ETL_TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_DSN, reason="ETL_TEST_DATABASE_URL is not configured"),
]


def test_postgres_schema_health_and_idempotent_load(tmp_path):
    psycopg = pytest.importorskip("psycopg")
    settings = Settings(
        database_url=SecretStr(TEST_DSN),
        quarantine_dir=tmp_path / "quarantine",
        metrics_dir=tmp_path / "metrics",
        data_dir=tmp_path,
    )
    loader = Loader(settings)
    loader.initialize_schema()
    assert loader.health_check() is True

    event = EventRow(
        event_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        event_name="login",
        occurred_at=datetime.now(timezone.utc),
        properties={"integration_test": True},
    )
    try:
        assert loader.load([event]) == 1
        assert loader.load([event]) == 0
        with psycopg.connect(TEST_DSN) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM events WHERE event_id = %s",
                    (event.event_id,),
                )
                assert cursor.fetchone()[0] == 1
    finally:
        with psycopg.connect(TEST_DSN) as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM events WHERE event_id = %s", (event.event_id,))
                cursor.execute("DELETE FROM users WHERE user_id = %s", (event.user_id,))
