"""Isolated unit tests for PostgreSQL loading behavior."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from etl.exceptions import LoadError
from etl.load import Loader
from etl.transform import EventRow


class FakeCursor:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.rowcount = -1
        self.queries: list[str] = []
        self.params: list[list[dict]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def executemany(self, query, params):
        if self.fail:
            raise RuntimeError("simulated database failure")
        self.queries.append(query)
        values = list(params)
        self.params.append(values)
        self.rowcount = len(values) if "INSERT INTO events" in query else len(values)

    def execute(self, query, params=None):
        if self.fail:
            raise RuntimeError("simulated database failure")
        self.queries.append(query)
        self.rowcount = 0


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self._cursor


def _event_row() -> EventRow:
    return EventRow(
        event_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        event_name="login",
        occurred_at=datetime.now(timezone.utc) - timedelta(hours=1),
        session_id=uuid.uuid4(),
        properties={"ip": "192.0.2.1"},
    )


@pytest.mark.unit
def test_load_inserts_users_before_events(monkeypatch, settings):
    import etl.load as load_module

    cursor = FakeCursor()
    fake_driver = SimpleNamespace(
        connect=lambda *a, **k: FakeConnection(cursor),
        types=SimpleNamespace(json=SimpleNamespace(Jsonb=lambda value: value)),
    )
    monkeypatch.setattr(load_module, "psycopg", fake_driver)
    loader = Loader(settings)
    inserted = loader.load([_event_row(), _event_row()])
    assert inserted == 2
    assert len(cursor.queries) == 2
    assert "INSERT INTO users" in cursor.queries[0]
    assert "INSERT INTO events" in cursor.queries[1]


@pytest.mark.unit
def test_load_empty_batch_is_noop(monkeypatch, settings):
    import etl.load as load_module

    def should_not_connect(*_args, **_kwargs):
        raise AssertionError("empty load should not connect")

    monkeypatch.setattr(load_module, "psycopg", SimpleNamespace(connect=should_not_connect))
    assert Loader(settings).load([]) == 0


@pytest.mark.unit
def test_load_database_failure_is_wrapped(monkeypatch, settings):
    import etl.load as load_module

    cursor = FakeCursor(fail=True)
    fake_driver = SimpleNamespace(connect=lambda *a, **k: FakeConnection(cursor))
    monkeypatch.setattr(load_module, "psycopg", fake_driver)
    with pytest.raises(LoadError, match="PostgreSQL load failed"):
        Loader(settings).load([_event_row()])


@pytest.mark.unit
def test_health_check_validates_schema_even_when_table_is_empty(monkeypatch, settings):
    import etl.load as load_module

    cursor = FakeCursor()
    fake_driver = SimpleNamespace(connect=lambda *a, **k: FakeConnection(cursor))
    monkeypatch.setattr(load_module, "psycopg", fake_driver)
    assert Loader(settings).health_check() is True
    assert "LIMIT 0" in cursor.queries[0]


@pytest.mark.unit
def test_health_check_failure_is_not_reported_as_healthy(monkeypatch, settings):
    import etl.load as load_module

    cursor = FakeCursor(fail=True)
    fake_driver = SimpleNamespace(connect=lambda *a, **k: FakeConnection(cursor))
    monkeypatch.setattr(load_module, "psycopg", fake_driver)
    with pytest.raises(LoadError, match="health check failed"):
        Loader(settings).health_check()


@pytest.mark.unit
def test_missing_postgres_driver_fails_with_actionable_error(monkeypatch, settings):
    import etl.load as load_module

    def missing_driver(_module_name):
        raise ImportError("simulated missing driver")

    monkeypatch.setattr(load_module, "psycopg", None)
    monkeypatch.setattr(load_module, "import_module", missing_driver)
    with pytest.raises(LoadError, match="install the runtime dependencies"):
        Loader(settings).health_check()


@pytest.mark.unit
def test_initialize_schema_executes_each_statement(monkeypatch, settings):
    import etl.load as load_module

    cursor = FakeCursor()
    monkeypatch.setattr(
        load_module,
        "psycopg",
        SimpleNamespace(connect=lambda *a, **k: FakeConnection(cursor)),
    )
    Loader(settings).initialize_schema()
    assert len(cursor.queries) == 5
    assert any("CREATE TABLE IF NOT EXISTS users" in query for query in cursor.queries)
    assert any("CREATE TABLE IF NOT EXISTS events" in query for query in cursor.queries)
