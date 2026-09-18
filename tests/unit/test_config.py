import os

import pytest

from pipeline.config import (
    ConfigurationError,
    load_runtime_environment,
)

VALID_PRODUCTION_ENVIRONMENT = {
    "PIPELINE_ENV": "prod",
    "OPENFOODFACTS_USER_AGENT": (
        "portfolio-data-pipeline/1.0 (https://github.com/dwaltenbaugh/portfolio-data-pipeline)"
    ),
    "OPENFOODFACTS_INITIAL_WATERMARK": ("2026-01-01T00:00:00+00:00"),
    "OPENFOODFACTS_LOOKBACK_HOURS": "24",
    "AWS_DEFAULT_REGION": "us-east-1",
    "RAW_BUCKET": "portfolio-data-prod",
    "PIPELINE_DB_NAME": "portfolio",
    "PIPELINE_DB_USER": "portfolio_user",
    "PIPELINE_DB_PASSWORD": "production-secret",
    "PIPELINE_DB_HOST": "prod-db.internal",
    "PIPELINE_DB_PORT": "5432",
}


def set_valid_production_environment(monkeypatch) -> None:
    for name, value in VALID_PRODUCTION_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)

    monkeypatch.delenv(
        "S3_ENDPOINT_URL",
        raising=False,
    )


def test_development_loads_selected_env_file(
    monkeypatch,
    tmp_path,
) -> None:
    env_file = tmp_path / ".env.dev"
    env_file.write_text(
        "EXAMPLE_SETTING=loaded-from-dev-file\n",
        encoding="utf-8",
    )

    monkeypatch.delenv(
        "PIPELINE_ENV",
        raising=False,
    )
    monkeypatch.delenv(
        "EXAMPLE_SETTING",
        raising=False,
    )
    monkeypatch.setenv(
        "ENV_FILE_PATH",
        str(env_file),
    )

    environment = load_runtime_environment()

    assert environment == "dev"
    assert os.environ["PIPELINE_ENV"] == "dev"
    assert os.environ["EXAMPLE_SETTING"] == "loaded-from-dev-file"


def test_production_does_not_load_env_file(
    monkeypatch,
    tmp_path,
) -> None:
    set_valid_production_environment(monkeypatch)

    env_file = tmp_path / ".env.prod"
    env_file.write_text(
        "EXAMPLE_SETTING=should-not-be-loaded\n",
        encoding="utf-8",
    )

    monkeypatch.setenv(
        "ENV_FILE_PATH",
        str(env_file),
    )
    monkeypatch.delenv(
        "EXAMPLE_SETTING",
        raising=False,
    )

    environment = load_runtime_environment()

    assert environment == "prod"
    assert "EXAMPLE_SETTING" not in os.environ


def test_unknown_environment_is_rejected(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "PIPELINE_ENV",
        "staging",
    )

    with pytest.raises(
        ConfigurationError,
        match="dev.*prod",
    ):
        load_runtime_environment()


def test_production_rejects_missing_required_setting(
    monkeypatch,
) -> None:
    set_valid_production_environment(monkeypatch)

    monkeypatch.delenv(
        "RAW_BUCKET",
        raising=False,
    )

    with pytest.raises(
        ConfigurationError,
        match="RAW_BUCKET",
    ):
        load_runtime_environment()


@pytest.mark.parametrize(
    "database_host",
    [
        "localhost",
        "127.0.0.1",
        "pipeline-db",
    ],
)
def test_production_rejects_local_database_host(
    monkeypatch,
    database_host,
) -> None:
    set_valid_production_environment(monkeypatch)
    monkeypatch.setenv(
        "PIPELINE_DB_HOST",
        database_host,
    )

    with pytest.raises(
        ConfigurationError,
        match="PIPELINE_DB_HOST",
    ):
        load_runtime_environment()


def test_production_rejects_s3_endpoint_override(
    monkeypatch,
) -> None:
    set_valid_production_environment(monkeypatch)
    monkeypatch.setenv(
        "S3_ENDPOINT_URL",
        "http://minio:9000",
    )

    with pytest.raises(
        ConfigurationError,
        match="S3_ENDPOINT_URL",
    ):
        load_runtime_environment()
