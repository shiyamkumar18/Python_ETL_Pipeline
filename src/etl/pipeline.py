"""Pipeline orchestration: extract → transform → load."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_settings
from .exceptions import ETLError, QuarantineError
from .extract import Extractor
from .extract_file import FileExtractor
from .load import Loader
from .logging_config import configure_logging
from .transform import Transformer

log = logging.getLogger("etl.pipeline")


def run_once(config_path: str | None = None, max_batches: int | None = None) -> dict[str, Any]:
    """Execute one pipeline run and return a machine-readable run summary."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()

    metrics: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "extracted": 0,
        "valid": 0,
        "rejected": 0,
        "reject_rate_pct": 0.0,
        "loaded": 0,
    }

    try:
        settings = load_settings(config_path)
        configure_logging(settings.log_level)
        log.info("pipeline start", extra={"run_id": run_id, "stage": "start"})

        if settings.source_type == "file":
            raw_rows = FileExtractor(settings).fetch_all(max_batches=max_batches)
        else:
            async def _extract() -> list[dict]:
                async with Extractor(settings) as extractor:
                    return await extractor.fetch_all(max_batches=max_batches)

            raw_rows = asyncio.run(_extract())
        metrics["extracted"] = len(raw_rows)

        transformer = Transformer(settings, run_id=run_id)
        try:
            valid_rows = transformer.transform(raw_rows)
        except QuarantineError as exc:
            metrics.update(
                {
                    "valid": exc.valid_count,
                    "rejected": exc.rejected_count,
                    "reject_rate_pct": exc.reject_rate_pct,
                }
            )
            raise

        metrics["valid"] = len(valid_rows)
        metrics["rejected"] = metrics["extracted"] - metrics["valid"]
        metrics["reject_rate_pct"] = round(
            metrics["rejected"] * 100.0 / metrics["extracted"], 2
        ) if metrics["extracted"] else 0.0

        loader = Loader(settings)
        loader.health_check()
        metrics["loaded"] = loader.load(valid_rows)
        metrics["status"] = "ok"

    except QuarantineError as exc:
        metrics["status"] = "quarantine_threshold_exceeded"
        metrics["error"] = str(exc)
        log.error("pipeline rejected by data-quality gate: %s", exc, extra={"run_id": run_id})
    except ETLError as exc:
        metrics["extracted"] = max(
            metrics["extracted"], getattr(exc, "rows_extracted", 0)
        )
        metrics["status"] = "error"
        metrics["error"] = str(exc)
        log.exception("pipeline failed", extra={"run_id": run_id})
    except Exception as exc:
        metrics["status"] = "error"
        metrics["error"] = f"{type(exc).__name__}: {exc}"
        log.exception("unexpected pipeline failure", extra={"run_id": run_id})
    finally:
        metrics["duration_ms"] = int((time.monotonic() - started) * 1000)
        metrics["finished_at"] = datetime.now(timezone.utc).isoformat()
        try:
            # Settings may fail to load; in that case there is no configured sink.
            if "settings" in locals():
                _write_metrics(settings, metrics)
        except OSError:
            log.exception("could not persist pipeline metrics", extra={"run_id": run_id})

    if metrics.get("status") == "ok":
        log.info(
            "pipeline complete",
            extra={
                "run_id": run_id,
                "stage": "end",
                "duration_ms": metrics["duration_ms"],
                "rows_in": metrics["extracted"],
                "rows_out": metrics["loaded"],
            },
        )
    return metrics


def _write_metrics(settings: Any, metrics: dict[str, Any]) -> None:
    """Append one JSON object per run to a UTC-date-partitioned metrics file."""
    settings.metrics_dir.mkdir(parents=True, exist_ok=True)
    path: Path = settings.metrics_dir / (
        f"{datetime.now(timezone.utc).date().isoformat()}.jsonl"
    )
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(metrics, ensure_ascii=False, default=str) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(prog="etl.pipeline")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit (default)")
    parser.add_argument("--schedule", type=str, default=None, help="Cron expression (UTC)")
    parser.add_argument("--config", type=str, default=None, help="Path to YAML configuration")
    parser.add_argument("--max-batches", type=int, default=None, help="Cap pages for a run")
    args = parser.parse_args()

    if args.max_batches is not None and args.max_batches < 1:
        parser.error("--max-batches must be at least 1")

    if args.schedule:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger

        scheduler = BlockingScheduler(timezone="UTC")
        scheduler.add_job(
            lambda: run_once(args.config, args.max_batches),
            CronTrigger.from_crontab(args.schedule),
            max_instances=1,
            coalesce=True,
        )
        logging.getLogger(__name__).info("starting scheduler: %s", args.schedule)
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass
        return 0

    result = run_once(args.config, args.max_batches)
    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
