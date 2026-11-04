"""Validate, normalize, deduplicate and quarantine raw event records."""
from __future__ import annotations

import json
import logging
import math
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .config import Settings
from .exceptions import QuarantineError

log = logging.getLogger(__name__)

ALLOWED_EVENT_NAMES = {
    "signup", "login", "logout", "feature_use",
    "plan_change", "invoice_viewed", "payment_failed",
    "subscription_canceled",
}


class EventRow(BaseModel):
    """Validated canonical event row, matching the PostgreSQL schema."""

    model_config = ConfigDict(extra="ignore")

    event_id: uuid.UUID
    user_id: uuid.UUID
    event_name: str
    occurred_at: datetime
    session_id: uuid.UUID | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("event_name")
    @classmethod
    def _check_event_name(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in ALLOWED_EVENT_NAMES:
            raise ValueError(f"unknown event_name: {value}")
        return normalized

    @field_validator("occurred_at", "ingested_at")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must include a timezone")
        return value.astimezone(timezone.utc)


class Transformer:
    """Stateful transformer for a single pipeline run."""

    def __init__(self, settings: Settings, run_id: str):
        self.settings = settings
        self.run_id = run_id
        self._seen_event_ids: set[uuid.UUID] = set()
        self.quarantine_path: Path = (
            settings.quarantine_dir
            / f"{datetime.now(timezone.utc).date().isoformat()}.jsonl"
        )
        self.quarantine_path.parent.mkdir(parents=True, exist_ok=True)

    def transform(self, rows: Iterable[Mapping[str, Any]]) -> list[EventRow]:
        """Return valid rows or raise with complete rejection statistics.

        Input iterables are materialized exactly once, so generators are handled
        correctly and denominator/counts cannot drift during processing.
        """
        raw_rows = list(rows)
        valid: list[EventRow] = []
        rejected = 0
        max_age = timedelta(days=self.settings.reject_event_age_days)
        now = datetime.now(timezone.utc)

        for raw in raw_rows:
            if not isinstance(raw, Mapping):
                self._quarantine({"value": repr(raw)}, "record_not_a_mapping")
                rejected += 1
                continue
            payload = dict(raw)
            try:
                event = EventRow.model_validate(payload)
                if event.occurred_at > now:
                    raise ValueError("occurred_at_in_the_future")
                if now - event.occurred_at > max_age:
                    raise ValueError("event_too_old")
                if event.event_id in self._seen_event_ids:
                    raise ValueError("duplicate_event_id_in_batch")
                self._validate_properties(event.properties)
            except ValidationError as exc:
                self._quarantine(payload, "validation_error", details=exc.errors())
                rejected += 1
                continue
            except ValueError as exc:
                self._quarantine(payload, str(exc))
                rejected += 1
                continue

            self._seen_event_ids.add(event.event_id)
            valid.append(event)

        total = len(raw_rows)
        reject_pct = (rejected * 100.0 / total) if total else 0.0
        log.info(
            "transform complete",
            extra={
                "stage": "transform",
                "rows_in": total,
                "rows_out": len(valid),
                "rejected": rejected,
                "reject_pct": round(reject_pct, 2),
                "run_id": self.run_id,
            },
        )
        if reject_pct > self.settings.reject_rate_threshold_pct:
            raise QuarantineError(
                (
                    f"reject rate {reject_pct:.1f}% exceeds threshold "
                    f"{self.settings.reject_rate_threshold_pct:.1f}%"
                ),
                total_count=total,
                valid_count=len(valid),
                rejected_count=rejected,
                reject_rate_pct=round(reject_pct, 2),
            )
        return valid

    def _validate_properties(self, properties: dict[str, Any]) -> None:
        if len(properties) > self.settings.max_properties_keys:
            raise ValueError("too_many_property_keys")
        try:
            serialized = json.dumps(
                properties,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"properties_not_valid_json: {exc}") from exc
        if len(serialized.encode("utf-8")) > self.settings.max_properties_bytes:
            raise ValueError("properties_payload_too_large")
        if not self._all_finite_numbers(properties):
            raise ValueError("properties_contain_non_finite_number")

    @classmethod
    def _all_finite_numbers(cls, value: Any) -> bool:
        if isinstance(value, float):
            return math.isfinite(value)
        if isinstance(value, dict):
            return all(
                cls._all_finite_numbers(key) and cls._all_finite_numbers(item)
                for key, item in value.items()
            )
        if isinstance(value, list):
            return all(cls._all_finite_numbers(item) for item in value)
        return True

    def _quarantine(
        self,
        raw: dict[str, Any],
        reason: str,
        *,
        details: Any | None = None,
    ) -> None:
        entry = {
            "run_id": self.run_id,
            "quarantined_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "payload": raw,
        }
        if details is not None:
            entry["details"] = details
        try:
            with self.quarantine_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        except OSError as exc:
            log.exception("could not write quarantine record")
            raise RuntimeError(f"failed to write quarantine record: {exc}") from exc
