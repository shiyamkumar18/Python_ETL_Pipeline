"""Tests for stable structured logging output."""
from __future__ import annotations

import json
import logging

import pytest

from etl.logging_config import JsonFormatter, configure_logging


@pytest.mark.unit
def test_json_formatter_includes_context_fields():
    record = logging.LogRecord("etl.test", logging.INFO, "x.py", 1, "hello", (), None)
    record.run_id = "run-123"
    record.stage = "extract"
    record.rows_in = 4
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "hello"
    assert payload["run_id"] == "run-123"
    assert payload["rows_in"] == 4


@pytest.mark.unit
def test_configure_logging_does_not_accumulate_handlers():
    root = configure_logging("WARNING")
    first_count = len(root.handlers)
    configure_logging("INFO")
    assert len(root.handlers) == first_count == 1
    assert root.level == logging.INFO
