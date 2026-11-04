-- Standalone schema for the Python ETL Pipeline portfolio project.
-- PostgreSQL executes this file automatically on first Docker volume creation.

CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE RESTRICT,
    event_name TEXT NOT NULL CHECK (
        event_name IN (
            'signup', 'login', 'logout', 'feature_use',
            'plan_change', 'invoice_viewed', 'payment_failed',
            'subscription_canceled'
        )
    ),
    occurred_at TIMESTAMPTZ NOT NULL,
    session_id UUID,
    properties JSONB NOT NULL DEFAULT '{}'::JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_user_occurred_at
    ON events (user_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_name_occurred_at
    ON events (event_name, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_occurred_at_brin
    ON events USING BRIN (occurred_at);
