"""Pipeline orchestration tests with isolated API and database boundaries."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest

from etl.exceptions import ExtractError, LoadError
from etl.pipeline import run_once


class FakeExtractor:
    rows = []
    error = None

    def __init__(self, _settings):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def fetch_all(self, max_batches=None):
        if self.error:
            raise self.error
        return self.rows


class FakeLoader:
    inserted = 0

    def __init__(self, _settings):
        pass

    def health_check(self):
        return True

    def load(self, rows):
        return self.inserted if rows else 0


def _make_event(event_name="login"):
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "event_name": event_name,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "session_id": str(uuid.uuid4()),
        "properties": {"source": "test"},
    }


@pytest.mark.unit
def test_pipeline_run_succeeds_and_persists_metrics(monkeypatch, settings):
    import etl.pipeline as pipeline_module

    FakeExtractor.rows = [_make_event() for _ in range(5)]
    FakeExtractor.error = None
    FakeLoader.inserted = 5
    monkeypatch.setattr(pipeline_module, "load_settings", lambda _path=None: settings)
    monkeypatch.setattr(pipeline_module, "Extractor", FakeExtractor)
    monkeypatch.setattr(pipeline_module, "Loader", FakeLoader)

    metrics = run_once()
    assert metrics["status"] == "ok"
    assert metrics["extracted"] == 5
    assert metrics["valid"] == 5
    assert metrics["rejected"] == 0
    assert metrics["loaded"] == 5
    files = list(settings.metrics_dir.glob("*.jsonl"))
    assert len(files) == 1
    assert json.loads(files[0].read_text().splitlines()[0])["status"] == "ok"


@pytest.mark.unit
def test_pipeline_records_transform_stats_when_quality_gate_fails(monkeypatch, settings):
    import etl.pipeline as pipeline_module

    FakeExtractor.rows = [_make_event()] + [_make_event("unknown") for _ in range(9)]
    FakeExtractor.error = None
    settings.reject_rate_threshold_pct = 5.0
    monkeypatch.setattr(pipeline_module, "load_settings", lambda _path=None: settings)
    monkeypatch.setattr(pipeline_module, "Extractor", FakeExtractor)
    monkeypatch.setattr(pipeline_module, "Loader", FakeLoader)

    metrics = run_once()
    assert metrics["status"] == "quarantine_threshold_exceeded"
    assert metrics["extracted"] == 10
    assert metrics["valid"] == 1
    assert metrics["rejected"] == 9
    assert metrics["reject_rate_pct"] == 90.0
    assert metrics["loaded"] == 0


@pytest.mark.unit
def test_pipeline_records_extraction_failure(monkeypatch, settings):
    import etl.pipeline as pipeline_module

    FakeExtractor.rows = []
    FakeExtractor.error = ExtractError("source unavailable")
    monkeypatch.setattr(pipeline_module, "load_settings", lambda _path=None: settings)
    monkeypatch.setattr(pipeline_module, "Extractor", FakeExtractor)

    metrics = run_once()
    assert metrics["status"] == "error"
    assert "source unavailable" in metrics["error"]
    assert metrics["extracted"] == 0


@pytest.mark.unit
def test_pipeline_preserves_partial_extract_count(monkeypatch, settings):
    import etl.pipeline as pipeline_module

    FakeExtractor.rows = []
    FakeExtractor.error = ExtractError("page 2 failed", rows_extracted=17)
    monkeypatch.setattr(pipeline_module, "load_settings", lambda _path=None: settings)
    monkeypatch.setattr(pipeline_module, "Extractor", FakeExtractor)

    metrics = run_once()
    assert metrics["status"] == "error"
    assert metrics["extracted"] == 17
    assert metrics["valid"] == 0
    assert metrics["loaded"] == 0


@pytest.mark.unit
def test_pipeline_reports_database_load_error(monkeypatch, settings):
    import etl.pipeline as pipeline_module

    class BrokenLoader:
        def __init__(self, _settings):
            pass
        def health_check(self):
            return True
        def load(self, _rows):
            raise LoadError("database unavailable")

    FakeExtractor.rows = [_make_event() for _ in range(2)]
    FakeExtractor.error = None
    monkeypatch.setattr(pipeline_module, "load_settings", lambda _path=None: settings)
    monkeypatch.setattr(pipeline_module, "Extractor", FakeExtractor)
    monkeypatch.setattr(pipeline_module, "Loader", BrokenLoader)

    metrics = run_once()
    assert metrics["status"] == "error"
    assert metrics["extracted"] == 2
    assert metrics["valid"] == 2
    assert metrics["loaded"] == 0
    assert "database unavailable" in metrics["error"]


@pytest.mark.unit
def test_pipeline_stops_when_database_health_check_fails(monkeypatch, settings):
    import etl.pipeline as pipeline_module

    class UnhealthyLoader:
        def __init__(self, _settings):
            pass
        def health_check(self):
            raise LoadError("events table missing")
        def load(self, _rows):
            raise AssertionError("load must not run after failed health check")

    FakeExtractor.rows = [_make_event()]
    FakeExtractor.error = None
    monkeypatch.setattr(pipeline_module, "load_settings", lambda _path=None: settings)
    monkeypatch.setattr(pipeline_module, "Extractor", FakeExtractor)
    monkeypatch.setattr(pipeline_module, "Loader", UnhealthyLoader)

    metrics = run_once()
    assert metrics["status"] == "error"
    assert metrics["valid"] == 1
    assert metrics["loaded"] == 0
    assert "events table missing" in metrics["error"]


@pytest.mark.unit
def test_pipeline_supports_file_source(monkeypatch, settings):
    import etl.pipeline as pipeline_module

    class FakeFileExtractor:
        def __init__(self, _settings):
            pass
        def fetch_all(self, max_batches=None):
            return [_make_event()]

    settings.source_type = "file"
    FakeLoader.inserted = 1
    monkeypatch.setattr(pipeline_module, "load_settings", lambda _path=None: settings)
    monkeypatch.setattr(pipeline_module, "FileExtractor", FakeFileExtractor)
    monkeypatch.setattr(
        pipeline_module,
        "Extractor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API source must not run")),
    )
    monkeypatch.setattr(pipeline_module, "Loader", FakeLoader)

    metrics = run_once()
    assert metrics["status"] == "ok"
    assert metrics["extracted"] == 1
    assert metrics["valid"] == 1
    assert metrics["loaded"] == 1
