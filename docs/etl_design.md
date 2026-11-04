# ETL Design Notes

## Stage boundaries

```text
HTTP API or JSON/JSONL/NDJSON/CSV file → extraction → transformation → PostgreSQL loading
                    │             │
                    │             └── JSONL quarantine
                    └── retry/backoff

Pipeline orchestration → structured logs + JSONL run metrics
```

Each stage has a narrow contract: the extractor returns validated API page structures, the transformer returns typed `EventRow` objects or a data-quality exception with counts, and the loader persists those rows transactionally.

## Extraction guarantees

The source is selectable through `source_type=api|file`. API mode uses the async paginated client; file mode supports JSON arrays, `{ "data": [...] }` payloads, newline-delimited JSON, and CSV with JSON-encoded `properties`. File records are structurally checked before entering the common transformer.

- Async HTTP client with explicit timeout.
- Exponential retry for connection/request failures and HTTP 5xx only.
- No retries for ordinary 4xx responses.
- Strict page payload validation.
- Uses `has_more` and `next_offset` when supplied; detects empty pages that claim more data and offsets that fail to advance.
- `iter_batches()` exposes a page iterator for consumers that need to avoid one large extraction list. The current orchestrator uses `fetch_all()` for this small demonstration dataset.

## Transformation and data quality

Pydantic validates UUID fields, event names, and timestamp types. Timestamps must be timezone-aware and are normalized to UTC. Semantic checks reject future events, events older than the configured retention window, duplicate event identifiers within a run, property objects exceeding configured key/byte limits, and non-JSON or non-finite property values.

Rejected rows are written to a date-partitioned JSONL quarantine stream with a run ID, rejection timestamp, reason, and original payload. The transformer computes counts from a materialized input iterable exactly once. When the configured reject-rate threshold is exceeded, `QuarantineError` carries total, valid, rejected, and percentage metrics so the pipeline does not lose counts on failure.

## PostgreSQL model and loading

The local schema has a `users` dimension and an `events` fact table. Events reference users through a foreign key. The loader inserts distinct user IDs first, then inserts events in bounded batches in the same transaction. Event primary-key conflicts are ignored, making re-runs idempotent.

The `health_check()` query selects the required columns with `LIMIT 0`. This validates connectivity and schema shape without incorrectly treating an empty table as unhealthy (or always returning healthy).

`sql/001_init.sql` is idempotent and is mounted into the PostgreSQL image's initialization directory by Compose. Existing named volumes are not reinitialized; use `docker compose down -v` only when intentionally resetting the local demo database.

## Metrics and logging

Each pipeline run emits a metrics record containing run ID, start/finish timestamps, extracted, valid, rejected, rejection percentage, loaded rows, status, duration, and error text when applicable. Metrics are appended to `data/metrics/YYYY-MM-DD.jsonl`.

Application logs are JSON lines. They include contextual run/stage fields and exception details, without serializing the database DSN.

## Failure semantics

| Failure | Behavior |
|---|---|
| Network error / HTTP 5xx | Retry with exponential backoff; fail extraction after configured attempts |
| HTTP 4xx | Fail extraction without retry |
| Invalid API payload or broken pagination | Fail extraction explicitly |
| Invalid event | Quarantine and continue within the batch |
| Rejection rate over threshold | Record all quality counts; skip loading; mark run as quality-gate failure |
| PostgreSQL unavailable or schema mismatch | Raise `LoadError`; transaction is rolled back |
| Duplicate event ID already in PostgreSQL | Ignore conflict; do not create duplicate event |
| Empty valid batch | Successful no-op load |

## Deliberate boundaries

This repository demonstrates batch ETL. It does not claim to provide streaming, CDC, exactly-once delivery across external systems, distributed locking, cloud deployment, or an enterprise scheduler. Incremental checkpointing and orchestration with Airflow are natural follow-on projects.
