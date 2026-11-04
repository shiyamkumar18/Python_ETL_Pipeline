"""Validated, environment-aware configuration for the ETL pipeline."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration.

    Environment variables use the ``ETL_`` prefix. When loading YAML,
    environment variables explicitly override the YAML values.
    """

    # Source
    source_type: Literal["api", "file"] = "api"
    source_file_path: Path = Path("data/sample_raw_events.json")
    source_api_base_url: str = "http://mock-api:8000"
    source_api_timeout_sec: float = Field(default=10.0, gt=0, le=300)
    source_api_max_retries: int = Field(default=4, ge=1, le=20)
    source_api_backoff_sec: float = Field(default=1.0, ge=0, le=60)
    source_batch_size: int = Field(default=500, ge=1, le=2000)

    # PostgreSQL
    database_url: SecretStr = SecretStr(
        "postgresql://subly:subly@postgres:5432/subly"
    )
    db_insert_batch_size: int = Field(default=500, ge=1, le=10000)

    # Data quality
    reject_event_age_days: int = Field(default=30, ge=0, le=36500)
    reject_rate_threshold_pct: float = Field(default=5.0, ge=0, le=100)
    max_properties_keys: int = Field(default=10, ge=0, le=1000)
    max_properties_bytes: int = Field(default=1024, ge=1, le=1_000_000)

    # Filesystem
    data_dir: Path = Path("data")
    quarantine_dir: Path = Path("data/quarantine")
    metrics_dir: Path = Path("data/metrics")

    # Logging
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_prefix="ETL_",
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )

    @field_validator("source_api_base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("source_api_base_url must start with http:// or https://")
        return value

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        normalized = value.upper().strip()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("log_level must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return normalized

    @classmethod
    def from_yaml(cls, path: str | os.PathLike[str]) -> "Settings":
        """Load YAML settings, with ``ETL_*`` environment variables taking priority."""
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        with config_path.open(encoding="utf-8") as stream:
            data: dict[str, Any] = yaml.safe_load(stream) or {}
        if not isinstance(data, dict):
            raise ValueError("YAML configuration must contain a mapping at its root")

        env_overrides = {
            key.upper().removeprefix("ETL_").lower(): value
            for key, value in os.environ.items()
            if key.upper().startswith("ETL_")
        }
        return cls.model_validate({**data, **env_overrides})


def load_settings(config_path: str | os.PathLike[str] | None = None) -> Settings:
    """Load settings from YAML when supplied, otherwise from the environment."""
    return Settings.from_yaml(config_path) if config_path else Settings()
