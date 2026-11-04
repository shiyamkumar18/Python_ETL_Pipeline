"""File-based extraction for JSON, JSONL/NDJSON and CSV event sources."""
from __future__ import annotations

import csv
import json
import logging
from typing import Any

from .config import Settings
from .exceptions import ExtractError

log = logging.getLogger(__name__)


class FileExtractor:
    """Read event records from a local structured data file."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.path = settings.source_file_path

    def fetch_all(self, max_batches: int | None = None) -> list[dict[str, Any]]:
        """Read records from a supported file, optionally applying a page-size cap."""
        if max_batches is not None and max_batches < 1:
            raise ValueError("max_batches must be at least 1")
        if not self.path.exists():
            raise ExtractError(f"source file does not exist: {self.path}")
        if not self.path.is_file():
            raise ExtractError(f"source path is not a file: {self.path}")

        suffix = self.path.suffix.lower()
        try:
            if suffix == ".json":
                rows = self._read_json()
            elif suffix in {".jsonl", ".ndjson"}:
                rows = self._read_json_lines()
            elif suffix == ".csv":
                rows = self._read_csv()
            else:
                raise ExtractError(
                    "unsupported source file extension "
                    f"{suffix!r}; use .json, .jsonl, .ndjson or .csv"
                )
        except ExtractError:
            raise
        except (OSError, UnicodeError, csv.Error, json.JSONDecodeError) as exc:
            raise ExtractError(f"could not read source file {self.path}: {exc}") from exc

        if max_batches is not None:
            rows = rows[: self.settings.source_batch_size * max_batches]
        log.info(
            "extracted file",
            extra={"stage": "extract", "rows_in": len(rows), "source_file": str(self.path)},
        )
        return rows

    def _read_json(self) -> list[dict[str, Any]]:
        with self.path.open(encoding="utf-8-sig") as stream:
            payload = json.load(stream)
        if isinstance(payload, dict):
            payload = payload.get("data")
        return self._validate_records(payload)

    def _read_json_lines(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with self.path.open(encoding="utf-8-sig") as stream:
            for line_number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ExtractError(
                        f"invalid JSONL at line {line_number}: {exc}",
                        rows_extracted=len(rows),
                    ) from exc
                if not isinstance(record, dict):
                    raise ExtractError(
                        f"JSONL line {line_number} must contain an object",
                        rows_extracted=len(rows),
                    )
                rows.append(record)
        return rows

    def _read_csv(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with self.path.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames:
                raise ExtractError("CSV source must contain a header row")
            required_columns = {"event_id", "user_id", "event_name", "occurred_at"}
            missing_columns = required_columns.difference(reader.fieldnames)
            if missing_columns:
                raise ExtractError(
                    "CSV source is missing required columns: "
                    + ", ".join(sorted(missing_columns))
                )
            for line_number, row in enumerate(reader, start=2):
                if None in row:
                    raise ExtractError(f"CSV line {line_number} has more values than headers")
                record: dict[str, Any] = dict(row)
                for key in ("event_id", "user_id", "event_name", "occurred_at"):
                    if record.get(key) is None:
                        raise ExtractError(
                            f"CSV line {line_number} is missing required column {key!r}"
                        )
                if not record.get("session_id"):
                    record["session_id"] = None
                properties = record.get("properties")
                if not properties:
                    record["properties"] = {}
                else:
                    try:
                        record["properties"] = json.loads(properties)
                    except json.JSONDecodeError as exc:
                        raise ExtractError(
                            "invalid properties JSON on CSV line "
                            f"{line_number}: {exc}",
                            rows_extracted=len(rows),
                        ) from exc
                rows.append(record)
        return rows

    @staticmethod
    def _validate_records(payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            raise ExtractError(
                "JSON source must be an array or an object containing a 'data' array"
            )
        if any(not isinstance(record, dict) for record in payload):
            raise ExtractError("every file-source record must be a JSON object")
        return payload
