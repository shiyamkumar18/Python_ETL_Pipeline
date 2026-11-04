"""Tests for validation, semantic checks, deduplication and quarantine."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from etl.exceptions import QuarantineError


@pytest.mark.unit
def test_valid_event_passes(transformer, make_event):
    rows = [make_event()]
    output = transformer.transform(rows)
    assert len(output) == 1
    assert output[0].event_name == "login"
    assert output[0].occurred_at.tzinfo is not None


@pytest.mark.unit
def test_event_name_is_normalized(transformer, make_event):
    output = transformer.transform([make_event(event_name=" LOGIN ")])
    assert output[0].event_name == "login"


@pytest.mark.unit
def test_unknown_event_name_is_quarantined(transformer, make_event):
    assert transformer.transform([make_event(event_name="unknown")]) == []
    entry = json.loads(transformer.quarantine_path.read_text().splitlines()[0])
    assert entry["reason"] == "validation_error"
    assert entry["run_id"] == "test-run"


@pytest.mark.unit
def test_future_event_is_quarantined(transformer, make_event):
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    assert transformer.transform([make_event(occurred_at=future)]) == []


@pytest.mark.unit
def test_naive_timestamp_is_quarantined(transformer, make_event):
    naive = (datetime.now() - timedelta(hours=1)).isoformat()
    assert transformer.transform([make_event(occurred_at=naive)]) == []


@pytest.mark.unit
def test_too_old_event_is_quarantined(transformer, make_event):
    old = (datetime.now(timezone.utc) - timedelta(days=400)).isoformat()
    assert transformer.transform([make_event(occurred_at=old)]) == []


@pytest.mark.unit
def test_duplicate_event_id_in_batch(transformer, make_event):
    event_id = str(uuid.uuid4())
    output = transformer.transform([
        make_event(event_id=event_id), make_event(event_id=event_id)
    ])
    assert len(output) == 1


@pytest.mark.unit
def test_duplicate_after_invalid_row_is_not_marked_seen(transformer, make_event):
    event_id = str(uuid.uuid4())
    rows = [
        make_event(event_id=event_id, event_name="invalid"),
        make_event(event_id=event_id, event_name="login"),
    ]
    output = transformer.transform(rows)
    assert len(output) == 1


@pytest.mark.unit
def test_too_many_property_keys_are_quarantined(transformer, make_event):
    event = make_event(properties={f"k{i}": i for i in range(11)})
    assert transformer.transform([event]) == []


@pytest.mark.unit
def test_property_payload_over_1kb_is_quarantined(transformer, make_event):
    event = make_event(properties={"payload": "x" * 1100})
    assert transformer.transform([event]) == []


@pytest.mark.unit
def test_non_finite_property_value_is_quarantined(transformer, make_event):
    event = make_event(properties={"score": float("nan")})
    assert transformer.transform([event]) == []


@pytest.mark.unit
def test_generator_input_is_counted_correctly(transformer, make_event):
    rows = (make_event() for _ in range(3))
    output = transformer.transform(rows)
    assert len(output) == 3


@pytest.mark.unit
def test_non_mapping_input_is_quarantined(transformer):
    assert transformer.transform(["not-an-object"]) == []


@pytest.mark.unit
def test_threshold_error_preserves_counts(transformer, make_event):
    transformer.settings.reject_rate_threshold_pct = 5.0
    rows = [make_event()] + [make_event(event_name="bogus") for _ in range(10)]
    with pytest.raises(QuarantineError) as caught:
        transformer.transform(rows)
    assert caught.value.total_count == 11
    assert caught.value.valid_count == 1
    assert caught.value.rejected_count == 10
    assert caught.value.reject_rate_pct == pytest.approx(90.91, abs=0.01)


@pytest.mark.unit
def test_empty_batch_has_zero_reject_rate(transformer):
    assert transformer.transform([]) == []
