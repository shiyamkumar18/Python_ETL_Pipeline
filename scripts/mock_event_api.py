"""Mock SaaS event API used as a reproducible ETL source."""
from __future__ import annotations

import os
import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

EVENT_NAMES = [
    "signup", "login", "logout", "feature_use",
    "plan_change", "invoice_viewed", "payment_failed",
    "subscription_canceled",
]
FEATURES = ["export", "dashboard", "report", "integration", "ai_assistant"]
USER_IDS = [str(uuid.uuid4()) for _ in range(200)]
_STORE: list[dict[str, Any]] = []


def _gen_event() -> dict[str, Any]:
    occurred_at = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, 14),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    name = random.choice(EVENT_NAMES)
    properties: dict[str, Any] = {}
    if name == "feature_use":
        properties = {
            "feature": random.choice(FEATURES),
            "duration_secs": random.randint(1, 600),
        }
    elif name == "login":
        properties = {"ip": f"192.0.2.{random.randint(1, 254)}"}

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "event_name": name,
        "occurred_at": occurred_at.isoformat(),
        "session_id": str(uuid.uuid4()),
        "properties": properties,
    }


def _seed() -> None:
    if not _STORE:
        random.seed(os.getenv("MOCK_API_SEED", "42"))
        _STORE.extend(_gen_event() for _ in range(5000))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _seed()
    yield


app = FastAPI(title="Subly Mock Event API", version="1.1.0", lifespan=lifespan)


@app.get("/events")
def list_events(
    offset: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=2000),
) -> dict[str, Any]:
    """Return a stable page of events with explicit continuation metadata."""
    window = _STORE[offset : offset + limit]
    next_offset = offset + len(window)
    return {
        "data": window,
        "offset": offset,
        "limit": limit,
        "total": len(_STORE),
        "next_offset": next_offset,
        "has_more": next_offset < len(_STORE),
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.middleware("http")
async def _flaky_middleware(request, call_next):
    """Optionally inject transient 503s to exercise client retry behavior."""
    try:
        flake_rate = float(os.getenv("MOCK_API_FLAKE_RATE", "0"))
    except ValueError:
        flake_rate = 0.0
    flake_rate = min(max(flake_rate, 0.0), 1.0)
    if request.url.path != "/health" and random.random() < flake_rate:
        return JSONResponse(status_code=503, content={"detail": "simulated transient failure"})
    return await call_next(request)
