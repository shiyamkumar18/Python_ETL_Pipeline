"""Load validated event rows into PostgreSQL using idempotent batches."""
from __future__ import annotations

import logging
from collections.abc import Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

from .config import Settings
from .exceptions import LoadError
from .transform import EventRow

log = logging.getLogger(__name__)
psycopg: Any | None = None
dict_row: Any | None = None


def _require_psycopg():
    """Import the PostgreSQL driver lazily so non-DB stages remain testable."""
    global psycopg, dict_row
    if psycopg is None:
        try:
            psycopg = import_module("psycopg")
            dict_row = import_module("psycopg.rows").dict_row
        except ImportError as exc:
            raise LoadError(
                "psycopg is not installed; install the runtime dependencies from requirements.txt"
            ) from exc
    return psycopg


class Loader:
    """Bulk-load events and their users with transaction-safe idempotency."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._dsn = settings.database_url.get_secret_value()

    def _connect(self):
        driver = _require_psycopg()
        return driver.connect(self._dsn, row_factory=dict_row)

    @staticmethod
    def _jsonb(value: dict):
        driver = _require_psycopg()
        return driver.types.json.Jsonb(value)

    @staticmethod
    def _chunks(items: Sequence[EventRow], size: int):
        for start in range(0, len(items), size):
            yield items[start : start + size]

    def initialize_schema(self, schema_path: str | Path | None = None) -> None:
        """Apply the repository's idempotent bootstrap SQL to the target DB."""
        path = Path(schema_path) if schema_path else (
            Path(__file__).resolve().parents[2] / "sql" / "001_init.sql"
        )
        try:
            sql = path.read_text(encoding="utf-8")
            statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    for statement in statements:
                        cursor.execute(statement)
        except Exception as exc:
            raise LoadError(f"schema initialization failed: {exc}") from exc

    def load(self, events: Sequence[EventRow]) -> int:
        """Insert users and events; return the number of newly inserted events."""
        if not events:
            log.info(
                "no events to load",
                extra={"stage": "load", "rows_in": 0, "rows_out": 0},
            )
            return 0

        users_sql = """
            INSERT INTO users (user_id)
            VALUES (%(user_id)s)
            ON CONFLICT (user_id) DO NOTHING
        """
        events_sql = """
            INSERT INTO events
                (event_id, user_id, event_name, occurred_at, session_id, properties, ingested_at)
            VALUES
                (%(event_id)s, %(user_id)s, %(event_name)s, %(occurred_at)s,
                 %(session_id)s, %(properties)s, %(ingested_at)s)
            ON CONFLICT (event_id) DO NOTHING
        """

        inserted = 0
        try:
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    unique_user_ids = {str(event.user_id) for event in events}
                    cursor.executemany(
                        users_sql,
                        [{"user_id": user_id} for user_id in unique_user_ids],
                    )
                    for batch in self._chunks(events, self.settings.db_insert_batch_size):
                        params = [
                            {
                                "event_id": str(event.event_id),
                                "user_id": str(event.user_id),
                                "event_name": event.event_name,
                                "occurred_at": event.occurred_at,
                                "session_id": str(event.session_id) if event.session_id else None,
                                "properties": self._jsonb(event.properties),
                                "ingested_at": event.ingested_at,
                            }
                            for event in batch
                        ]
                        cursor.executemany(events_sql, params)
                        if cursor.rowcount is not None and cursor.rowcount >= 0:
                            inserted += cursor.rowcount
        except Exception as exc:
            if isinstance(exc, LoadError):
                raise
            raise LoadError(f"PostgreSQL load failed: {exc}") from exc

        log.info(
            "load complete",
            extra={
                "stage": "load",
                "rows_in": len(events),
                "rows_out": inserted,
            },
        )
        return inserted

    def health_check(self) -> bool:
        """Return True only when the database and required event schema are usable.

        ``LIMIT 0`` deliberately validates the table/columns without requiring
        any data rows to exist.
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT user_id FROM users LIMIT 0"
                    )
                    cursor.execute(
                        "SELECT event_id, user_id, event_name, occurred_at, "
                        "session_id, properties, ingested_at FROM events LIMIT 0"
                    )
            return True
        except Exception as exc:
            raise LoadError(f"database health check failed: {exc}") from exc
