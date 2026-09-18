import os

import pytest

from pipeline.config import (
    ConfigurationError,
    load_runtime_environment,
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
    assert (
        os.environ["EXAMPLE_SETTING"]
        == "loaded-from-dev-file"
    )


def test_production_does_not_load_env_file(
    monkeypatch,
    tmp_path,
) -> None:
    env_file = tmp_path / ".env.prod"
    env_file.write_text(
        "EXAMPLE_SETTING=should-not-be-loaded\n",
        encoding="utf-8",
    )

    monkeypatch.setenv(
        "PIPELINE_ENV",
        "prod",
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