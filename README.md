# Python ETL Pipeline

> Portfolio project: a reproducible batch ETL pipeline that extracts SaaS events from a paginated HTTP API, validates and transforms records, quarantines invalid data, and loads the accepted records into PostgreSQL.

## Project status

The repository includes the complete local batch workflow, schema bootstrap, automated unit tests, optional PostgreSQL integration tests, Docker Compose setup, structured logs, per-run metrics, and GitHub Actions CI. Actual CI and live PostgreSQL execution results are produced by running the commands below; they are not implied by the source code alone.

## Latest verification snapshot

The current local validation run completed with **54 tests passed, 1 optional PostgreSQL integration test skipped, and 87% total statement/branch coverage**. Python compilation, YAML parsing, and the mock API health/pagination smoke test also passed. Live PostgreSQL, Docker Compose, Ruff, and GitHub Actions execution remain unverified in the current workspace. See [the detailed implementation status](docs/implementation_status.md) for component-level completion estimates and remaining checks.

## Architecture

```text
Mock Event API
     |
     v
HTTP extraction (pagination + retry/backoff)
     |
     v
Validation + normalization + in-run deduplication
     |                         \
     | valid                    \ invalid
     v                           v
PostgreSQL                 JSONL quarantine
     |
     v
Run metrics + structured JSON logs
```

**Sources:** `scripts/mock_event_api.py` exposes a stable, paginated SaaS event API. The file extractor also supports JSON arrays, JSON objects with a `data` array, JSONL/NDJSON, and CSV (with `properties` encoded as JSON).

**Sink:** PostgreSQL `users` and `events` tables. The loader creates missing user dimension rows before inserting events, preserving the event-to-user foreign key.

## Repository layout

```text
.
├── .github/workflows/ci.yml       # Python 3.11–3.13 test and lint workflow
├── configs/pipeline.yaml          # Non-secret runtime settings
├── data/sample_raw_events.json    # Sample source records
├── docs/etl_design.md             # Design decisions and failure behavior
├── scripts/mock_event_api.py      # Reproducible mock source API
├── sql/001_init.sql               # Idempotent PostgreSQL schema bootstrap
├── src/etl/
│   ├── config.py                  # Pydantic settings + YAML/env overrides
│   ├── exceptions.py              # Typed ETL exceptions
│   ├── extract.py                 # Async HTTP pagination + retry
│   ├── extract_file.py            # JSON / JSONL / NDJSON / CSV source reader
│   ├── transform.py               # Validation, semantic checks, dedupe, quarantine
│   ├── load.py                    # Transactional/idempotent PostgreSQL loading
│   ├── logging_config.py          # Structured JSON logs
│   └── pipeline.py                # Run orchestration and metrics
├── tests/                         # Unit tests + optional database integration test
├── Dockerfile
├── docker-compose.yml
├── requirements.txt               # Runtime dependencies
└── requirements-dev.txt           # Test and lint dependencies
```

## Quick start with Docker Compose

Requirements: Docker Engine and the Docker Compose plugin.

```bash
cp .env.example .env
docker compose up --build
```

Compose starts PostgreSQL, initializes the schema on a new database volume, starts the mock API, and runs one ETL cycle. Inspect the service output:

```bash
docker compose logs -f etl
docker compose logs -f mock-api
```

Run another pipeline cycle:

```bash
docker compose run --rm etl -m etl.pipeline --once
```

Run a bounded sample:

```bash
docker compose run --rm etl -m etl.pipeline --max-batches 2
```

The initial database schema is applied only when PostgreSQL creates a new data volume. To rebuild the demo database from scratch (this deletes its persisted data):

```bash
docker compose down -v
docker compose up --build
```

## Local development without Docker

Use Python 3.11 or later and a reachable PostgreSQL instance. Create the database and apply the schema:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
set -a; source .env; set +a
psql "$ETL_DATABASE_URL" -f sql/001_init.sql
```

Set the source and database connection settings, then start the mock API in one terminal for API mode:

```bash
export PYTHONPATH=src:.
export ETL_SOURCE_API_BASE_URL=http://localhost:8000
export ETL_DATABASE_URL=postgresql://subly:subly@localhost:5432/subly
python -m uvicorn scripts.mock_event_api:app --host 0.0.0.0 --port 8000
```

Run the ETL in a second terminal:

```bash
export PYTHONPATH=src:.
python -m etl.pipeline --once
```

To load the bundled JSON sample instead of the API, set `ETL_SOURCE_TYPE=file` (or set `source_type: file` in the YAML file) and run the same command. Set `ETL_SOURCE_FILE_PATH` to a `.json`, `.jsonl`, `.ndjson`, or `.csv` input file. For Docker Compose, the bundled sample is copied to `/app/data/sample_raw_events.json`. CSV files must have `event_id`, `user_id`, `event_name`, and `occurred_at` columns; `session_id` is optional and `properties` must contain JSON text.

For a recurring schedule (UTC cron syntax):

```bash
python -m etl.pipeline --schedule "*/5 * * * *"
```

## Configuration

Settings are read from environment variables with the `ETL_` prefix. YAML values can be loaded with `--config configs/pipeline.yaml`; environment variables override YAML values.

| Variable | Default | Purpose |
|---|---:|---|
| `ETL_SOURCE_TYPE` | `api` | Select `api` or `file` |
| `ETL_SOURCE_FILE_PATH` | `data/sample_raw_events.json` | Input path when `ETL_SOURCE_TYPE=file` |
| `ETL_SOURCE_API_BASE_URL` | `http://mock-api:8000` | Source API base URL |
| `ETL_SOURCE_API_TIMEOUT_SEC` | `10` | HTTP request timeout |
| `ETL_SOURCE_API_MAX_RETRIES` | `4` | Attempts for network and server errors |
| `ETL_SOURCE_API_BACKOFF_SEC` | `1` | Exponential backoff multiplier |
| `ETL_SOURCE_BATCH_SIZE` | `500` | API page size (1–2000) |
| `ETL_DATABASE_URL` | Compose PostgreSQL URL | PostgreSQL DSN (secret) |
| `ETL_DB_INSERT_BATCH_SIZE` | `500` | Insert page size |
| `ETL_REJECT_EVENT_AGE_DAYS` | `30` | Maximum accepted event age |
| `ETL_REJECT_RATE_THRESHOLD_PCT` | `5` | Abort load when rejection rate exceeds this percentage |
| `ETL_MAX_PROPERTIES_KEYS` | `10` | Maximum top-level event property count |
| `ETL_MAX_PROPERTIES_BYTES` | `1024` | Maximum serialized JSON property size |
| `ETL_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |

Never commit `.env` files or production credentials.

## Data quality rules

Each input record is checked for:

1. Required UUID identifiers and a supported event name.
2. Timezone-aware timestamps; events cannot be in the future or older than the configured age limit.
3. Unique `event_id` values within the current run.
4. A JSON-object `properties` payload with configurable key-count and serialized-byte limits.
5. JSON-compatible property values (non-finite numeric values are rejected).

Rejected records are appended to `data/quarantine/YYYY-MM-DD.jsonl`, with their reason and run ID. The transformer preserves total, valid, rejected, and rejection-rate statistics. If the configured rejection threshold is exceeded, the load stage is skipped and the run is marked `quarantine_threshold_exceeded`.

## Reliability and operational behavior

- **Retries:** transient network failures and HTTP 5xx responses are retried with exponential backoff. Ordinary 4xx responses are not retried.
- **Pagination:** `has_more`/`next_offset` are honored; malformed pages, empty pages that claim more data, and non-advancing offsets fail explicitly.
- **Idempotency:** PostgreSQL primary-key conflicts use `ON CONFLICT DO NOTHING`.
- **Referential integrity:** missing `users` dimension rows are inserted before event records in the same transaction.
- **Transactions:** a failed batch rolls back the transaction; the pipeline reports a typed load failure.
- **Health check:** the required event columns are checked using `LIMIT 0`, so an empty but correctly initialized table is healthy.
- **Metrics:** one JSON object per run is appended to `data/metrics/YYYY-MM-DD.jsonl`.
- **Logs:** JSON logs include run ID, stage, row counts, duration, and exception details when applicable.

## Testing

Install development dependencies:

```bash
pip install -r requirements-dev.txt
```

Run the isolated test suite:

```bash
pytest -m "not integration" -v
```

Run with coverage:

```bash
pytest -m "not integration" --cov=etl --cov-report=term-missing --cov-report=html
```

Run lint:

```bash
ruff check .
```

### PostgreSQL integration test

Set `ETL_TEST_DATABASE_URL` to a disposable PostgreSQL database. The integration test creates the schema, verifies health, inserts a generated event, checks duplicate-load behavior, and cleans up its test rows:

```bash
export ETL_TEST_DATABASE_URL=postgresql://subly:subly@localhost:5432/subly
pytest -m integration -v
```

The integration test is skipped when that variable is not set. Do not point it at a production database.

## Output locations

- Quarantine: `data/quarantine/YYYY-MM-DD.jsonl`
- Metrics: `data/metrics/YYYY-MM-DD.jsonl`
- PostgreSQL: `public.users`, `public.events`

## Scope and next extensions

This is intentionally a batch ETL portfolio project. It does not claim to implement streaming, CDC, distributed orchestration, or cloud deployment. Sensible next extensions include checkpointed incremental extraction, bounded-memory page-to-load processing for very large sources, Airflow orchestration, and a production secrets/observability stack.

## License

MIT — see [LICENSE](LICENSE).
