import os

from dotenv import load_dotenv


class ConfigurationError(ValueError):
    """Raised when runtime configuration is invalid."""


_PRODUCTION_REQUIRED_ENVIRONMENT_VARIABLES = (
    "OPENFOODFACTS_USER_AGENT",
    "OPENFOODFACTS_INITIAL_WATERMARK",
    "OPENFOODFACTS_LOOKBACK_HOURS",
    "AWS_DEFAULT_REGION",
    "RAW_BUCKET",
    "PIPELINE_DB_NAME",
    "PIPELINE_DB_USER",
    "PIPELINE_DB_PASSWORD",
    "PIPELINE_DB_HOST",
    "PIPELINE_DB_PORT",
)

_LOCAL_DATABASE_HOSTS = {
    "localhost",
    "127.0.0.1",
    "pipeline-db",
}


def _validate_production_environment() -> None:
    """Reject missing or development-only production configuration."""

    missing_variables = [
        variable_name
        for variable_name in (_PRODUCTION_REQUIRED_ENVIRONMENT_VARIABLES)
        if not os.getenv(
            variable_name,
            "",
        ).strip()
    ]

    if missing_variables:
        raise ConfigurationError(
            "Missing required production environment variables: " + ", ".join(missing_variables)
        )

    database_host = os.environ["PIPELINE_DB_HOST"].strip().lower()

    if database_host in _LOCAL_DATABASE_HOSTS:
        raise ConfigurationError(
            "PIPELINE_DB_HOST cannot reference a local development service in production."
        )

    if os.getenv(
        "S3_ENDPOINT_URL",
        "",
    ).strip():
        raise ConfigurationError(
            "S3_ENDPOINT_URL must not be set in production; production uses the AWS S3 endpoint."
        )


def load_runtime_environment() -> str:
    """
    Load local dotenv configuration for development.

    Production configuration must already be present in the process
    environment, normally injected by the deployment platform.
    """

    environment = (
        os.getenv(
            "PIPELINE_ENV",
            "dev",
        )
        .strip()
        .lower()
    )

    if environment == "dev":
        env_file_path = os.getenv(
            "ENV_FILE_PATH",
            ".env.dev",
        )

        load_dotenv(
            dotenv_path=env_file_path,
            override=False,
        )

    elif environment == "prod":
        _validate_production_environment()

    else:
        raise ConfigurationError("PIPELINE_ENV must be either 'dev' or 'prod'.")

    os.environ.setdefault(
        "PIPELINE_ENV",
        environment,
    )

    return environment
