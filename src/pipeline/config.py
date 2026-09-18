import os

from dotenv import load_dotenv


class ConfigurationError(ValueError):
    """Raised when runtime configuration is invalid."""


def load_runtime_environment() -> str:
    """
    Load local dotenv configuration for development.

    Production configuration must already be present in the process
    environment, normally injected by the deployment platform.
    """

    environment = os.getenv(
        "PIPELINE_ENV",
        "dev",
    ).strip().lower()

    if environment == "dev":
        env_file_path = os.getenv(
            "ENV_FILE_PATH",
            ".env.dev",
        )

        load_dotenv(
            dotenv_path=env_file_path,
            override=False,
        )

    elif environment != "prod":
        raise ConfigurationError(
            "PIPELINE_ENV must be either 'dev' or 'prod'."
        )

    os.environ.setdefault(
        "PIPELINE_ENV",
        environment,
    )

    return environment