"""Extract paginated event batches from the source API."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx

from .config import Settings
from .exceptions import ExtractError

log = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    """Retry network errors and server-side HTTP errors, never ordinary 4xx."""
    if isinstance(exc, httpx.RequestError):
        return True
    return (
        isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code >= 500
    )


class Extractor:
    """Async HTTP client with bounded retries and defensive pagination."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._client = httpx.AsyncClient(
            base_url=settings.source_api_base_url,
            timeout=settings.source_api_timeout_sec,
            headers={"Accept": "application/json"},
        )

    async def __aenter__(self) -> "Extractor":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self._client.aclose()

    async def fetch_batch(
        self, offset: int = 0, limit: int | None = None
    ) -> dict[str, Any]:
        """Fetch one page, returning its rows and pagination metadata."""
        if offset < 0:
            raise ValueError("offset must be non-negative")
        page_size = self.settings.source_batch_size if limit is None else limit
        if page_size < 1:
            raise ValueError("limit must be positive")
        url = f"/events?offset={offset}&limit={page_size}"

        payload: dict[str, Any] | None = None
        last_error: Exception | None = None
        for attempt in range(1, self.settings.source_api_max_retries + 1):
            try:
                response = await self._client.get(url)
                response.raise_for_status()
                candidate = response.json()
                if not isinstance(candidate, dict):
                    raise ValueError("API response must be a JSON object")
                rows = candidate.get("data")
                if not isinstance(rows, list):
                    raise ValueError("API response field 'data' must be a list")
                if any(not isinstance(row, dict) for row in rows):
                    raise ValueError("every API event must be a JSON object")
                if len(rows) > page_size:
                    raise ValueError("API returned more rows than the requested page size")
                payload = candidate
                break
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if not _is_retryable(exc) or attempt >= self.settings.source_api_max_retries:
                    break
                delay = min(
                    self.settings.source_api_backoff_sec * (2 ** (attempt - 1)),
                    30.0,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
            except Exception as exc:
                last_error = exc
                break

        if payload is None:
            assert last_error is not None
            raise ExtractError(f"GET {url} failed: {last_error}") from last_error

        rows = payload["data"]
        log.info(
            "extracted batch",
            extra={"stage": "extract", "rows_in": len(rows), "offset": offset},
        )
        return payload

    async def iter_batches(
        self, max_batches: int | None = None
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield pages until the API indicates there is no more data."""
        if max_batches is not None and max_batches < 1:
            raise ValueError("max_batches must be at least 1")

        offset = 0
        batch_number = 0
        page_size = self.settings.source_batch_size
        while True:
            payload = await self.fetch_batch(offset=offset, limit=page_size)
            rows: list[dict[str, Any]] = payload["data"]
            if not rows:
                if payload.get("has_more") is True:
                    raise ExtractError(
                        f"API returned an empty page at offset {offset} while has_more=true"
                    )
                break

            yield rows
            batch_number += 1
            if max_batches is not None and batch_number >= max_batches:
                break

            next_offset = payload.get("next_offset")
            expected_next_offset = offset + len(rows)
            if next_offset is None:
                next_offset = expected_next_offset
            if (
                isinstance(next_offset, bool)
                or not isinstance(next_offset, int)
                or next_offset != expected_next_offset
            ):
                raise ExtractError(
                    "API pagination returned an invalid next_offset: "
                    f"expected {expected_next_offset}, received {next_offset!r}"
                )
            offset = next_offset

            has_more = payload.get("has_more")
            if has_more is not None and not isinstance(has_more, bool):
                raise ExtractError(f"API has_more must be boolean, received {has_more!r}")
            if has_more is False:
                break
            if has_more is None and len(rows) < page_size:
                break

    async def fetch_all(self, max_batches: int | None = None) -> list[dict[str, Any]]:
        """Fetch all pages into memory. Prefer ``iter_batches`` for large sources."""
        output: list[dict[str, Any]] = []
        try:
            async for rows in self.iter_batches(max_batches=max_batches):
                output.extend(rows)
        except ExtractError as exc:
            exc.rows_extracted = len(output)
            raise
        return output


def fetch_all_sync(settings: Settings, max_batches: int | None = None) -> list[dict]:
    """Synchronous convenience wrapper for scripts and tests."""
    async def _run() -> list[dict]:
        async with Extractor(settings) as extractor:
            return await extractor.fetch_all(max_batches=max_batches)

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_run())
    raise RuntimeError("fetch_all_sync cannot run inside an active event loop; use Extractor")
