"""Configuration loading and validation tests."""
from __future__ import annotations

from pathlib import Path

import pytest

from etl.config import Settings, load_settings


@pytest.mark.unit
def test_yaml_values_are_loaded_and_environment_overrides(monkeypatch, tmp_path):
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        "source_api_base_url: http://yaml-api:8000\n"
        "source_batch_size: 100\n"
        "reject_rate_threshold_pct: 10\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("ETL_SOURCE_BATCH_SIZE", "250")
    settings = load_settings(config)
    assert settings.source_api_base_url == "http://yaml-api:8000"
    assert settings.source_batch_size == 250
    assert settings.reject_rate_threshold_pct == 10


@pytest.mark.unit
def test_missing_yaml_file_fails_clearly(tmp_path):
    with pytest.raises(FileNotFoundError):
        Settings.from_yaml(tmp_path / "missing.yaml")


@pytest.mark.unit
def test_invalid_configuration_is_rejected():
    with pytest.raises(ValueError):
        Settings(source_api_timeout_sec=0)
    with pytest.raises(ValueError):
        Settings(source_api_base_url="ftp://invalid")


@pytest.mark.unit
def test_local_defaults_write_inside_repository_data_dir():
    settings = Settings()
    assert settings.data_dir == Path("data")
    assert settings.quarantine_dir == Path("data/quarantine")
