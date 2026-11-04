"""Tests for HTTP extraction using httpx.MockTransport (no network required)."""
from __future__ import annotations

import asyncio

import httpx
import pytest

from etl.exceptions import ExtractError
from etl.extract import Extractor, fetch_all_sync


def _mock_http(monkeypatch, handler):
    original_client = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


@pytest.mark.unit
def test_fetch_all_paginates(monkeypatch, settings):
    calls = []
    def handler(request):
        calls.append(request.url.params.get("offset"))
        offset = int(request.url.params["offset"])
        if offset == 0:
            return httpx.Response(
                200,
                json={"data": [{"event_id": "a"}, {"event_id": "b"}], "has_more": True},
            )
        return httpx.Response(200, json={"data": [{"event_id": "c"}], "has_more": False})
    _mock_http(monkeypatch, handler)
    rows = fetch_all_sync(settings)
    assert [row["event_id"] for row in rows] == ["a", "b", "c"]
    assert calls == ["0", "2"]


@pytest.mark.unit
def test_extract_retries_persistent_5xx_then_raises(monkeypatch, settings):
    calls = 0
    def handler(_request):
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"detail": "unavailable"})
    _mock_http(monkeypatch, handler)
    with pytest.raises(ExtractError):
        fetch_all_sync(settings, max_batches=1)
    assert calls == settings.source_api_max_retries


@pytest.mark.unit
def test_extract_does_not_retry_4xx(monkeypatch, settings):
    calls = 0
    def handler(_request):
        nonlocal calls
        calls += 1
        return httpx.Response(400, json={"detail": "bad request"})
    _mock_http(monkeypatch, handler)
    with pytest.raises(ExtractError):
        fetch_all_sync(settings, max_batches=1)
    assert calls == 1


@pytest.mark.unit
def test_extract_rejects_malformed_response(monkeypatch, settings):
    _mock_http(monkeypatch, lambda _request: httpx.Response(200, json={"data": "not-a-list"}))
    with pytest.raises(ExtractError, match="data.*list"):
        fetch_all_sync(settings, max_batches=1)


@pytest.mark.unit
def test_extract_rejects_empty_page_with_has_more(monkeypatch, settings):
    _mock_http(
        monkeypatch,
        lambda _request: httpx.Response(200, json={"data": [], "has_more": True}),
    )
    with pytest.raises(ExtractError, match="empty page"):
        fetch_all_sync(settings)


@pytest.mark.unit
def test_invalid_page_limit_is_rejected(settings):
    async def run():
        async with Extractor(settings) as extractor:
            await extractor.fetch_batch(limit=0)
    with pytest.raises(ValueError, match="positive"):
        asyncio.run(run())


@pytest.mark.unit
def test_extract_tracks_partial_progress_on_later_page_failure(monkeypatch, settings):
    calls = 0
    def handler(_request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={"data": [{"event_id": "a"}, {"event_id": "b"}], "has_more": True},
            )
        return httpx.Response(503, json={"detail": "down"})
    _mock_http(monkeypatch, handler)
    with pytest.raises(ExtractError) as caught:
        fetch_all_sync(settings)
    assert caught.value.rows_extracted == 2


@pytest.mark.unit
def test_extract_rejects_non_boolean_has_more(monkeypatch, settings):
    _mock_http(monkeypatch, lambda _request: httpx.Response(
        200, json={"data": [{"event_id": "a"}, {"event_id": "b"}], "has_more": "yes"}
    ))
    with pytest.raises(ExtractError, match="has_more must be boolean"):
        fetch_all_sync(settings)
