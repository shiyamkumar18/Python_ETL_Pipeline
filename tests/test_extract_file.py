"""Tests for local file extraction formats."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from etl.exceptions import ExtractError
from etl.extract_file import FileExtractor


def _extractor(settings, path):
    settings.source_file_path = path
    return FileExtractor(settings)


@pytest.mark.unit
def test_json_array_file_is_extracted(settings, tmp_path):
    path = tmp_path / "events.json"
    rows = [{"event_id": "one"}, {"event_id": "two"}]
    path.write_text(json.dumps(rows), encoding="utf-8")
    assert _extractor(settings, path).fetch_all() == rows


@pytest.mark.unit
def test_json_data_envelope_is_supported(settings, tmp_path):
    path = tmp_path / "events.json"
    rows = [{"event_id": "one"}]
    path.write_text(json.dumps({"data": rows}), encoding="utf-8")
    assert _extractor(settings, path).fetch_all() == rows


@pytest.mark.unit
def test_json_file_with_non_object_records_is_rejected(settings, tmp_path):
    path = tmp_path / "events.json"
    path.write_text(json.dumps([{"event_id": "one"}, 123]), encoding="utf-8")
    with pytest.raises(ExtractError, match="every file-source record"):
        _extractor(settings, path).fetch_all()


@pytest.mark.unit
def test_jsonl_file_ignores_blank_lines(settings, tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text('{"event_id":"one"}\n\n{"event_id":"two"}\n', encoding="utf-8")
    assert [row["event_id"] for row in _extractor(settings, path).fetch_all()] == ["one", "two"]


@pytest.mark.unit
def test_invalid_jsonl_reports_partial_rows(settings, tmp_path):
    path = tmp_path / "events.ndjson"
    path.write_text('{"event_id":"one"}\nnot-json\n', encoding="utf-8")
    with pytest.raises(ExtractError) as caught:
        _extractor(settings, path).fetch_all()
    assert caught.value.rows_extracted == 1
    assert "line 2" in str(caught.value)


@pytest.mark.unit
def test_csv_file_parses_json_properties_and_empty_session(settings, tmp_path):
    path = tmp_path / "events.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            ["event_id", "user_id", "event_name", "occurred_at", "session_id", "properties"]
        )
        writer.writerow(
            ["one", "user-1", "login", "2026-10-15T09:30:00+00:00", "", '{"source":"csv"}']
        )
    rows = _extractor(settings, path).fetch_all()
    assert rows[0]["session_id"] is None
    assert rows[0]["properties"] == {"source": "csv"}


@pytest.mark.unit
def test_missing_and_unsupported_files_are_rejected(settings, tmp_path):
    with pytest.raises(ExtractError, match="does not exist"):
        _extractor(settings, tmp_path / "missing.json").fetch_all()

    unsupported = tmp_path / "events.parquet"
    unsupported.write_bytes(b"not-a-parquet-file")
    with pytest.raises(ExtractError, match="unsupported source file extension"):
        _extractor(settings, unsupported).fetch_all()


@pytest.mark.unit
def test_file_source_supports_batch_cap(settings, tmp_path):
    path = tmp_path / "events.json"
    rows = [{"event_id": str(index)} for index in range(5)]
    path.write_text(json.dumps(rows), encoding="utf-8")
    settings.source_batch_size = 2
    assert len(_extractor(settings, path).fetch_all(max_batches=2)) == 4


@pytest.mark.unit
def test_csv_requires_event_columns_even_when_empty(settings, tmp_path):
    path = tmp_path / "events.csv"
    path.write_text("event_id,event_name\n", encoding="utf-8")
    with pytest.raises(ExtractError, match="missing required columns"):
        _extractor(settings, path).fetch_all()


@pytest.mark.unit
def test_bundled_sample_file_passes_common_transform(settings):
    from etl.transform import Transformer

    settings.source_file_path = Path("data/sample_raw_events.json")
    settings.reject_event_age_days = 36500
    raw_rows = FileExtractor(settings).fetch_all()
    transformed = Transformer(settings, run_id="sample-file-test").transform(raw_rows)
    assert len(raw_rows) == 2
    assert len(transformed) == 2
