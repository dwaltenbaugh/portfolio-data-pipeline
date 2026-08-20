import os
from dataclasses import dataclass

import psycopg
from psycopg import Connection


@dataclass(frozen=True)
class PipelineDatabaseConfig:
    """
    Connection settings for the pipeline's PostgreSQL database.

    Keeping these settings together in a dataclass makes it easier
    to pass database configuration around as one object instead of
    passing five separate values to every function.
    """

    dbname: str
    user: str
    password: str
    host: str
    port: int


def load_pipeline_database_config() -> PipelineDatabaseConfig:
    """
    Build database configuration from environment variables.

    os.environ[...] is intentionally used for required settings.
    If one is missing, the application should fail immediately
    instead of silently connecting with an unexpected default.
    """

    return PipelineDatabaseConfig(
        dbname=os.environ["PIPELINE_DB_NAME"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
        host=os.environ["PIPELINE_DB_HOST"],

        # Environment variables are always strings.
        # psycopg expects the port as an integer, so convert it here.
        port=int(
            os.environ["PIPELINE_DB_PORT"]
        ),
    )


def connect_pipeline_database(
    config: PipelineDatabaseConfig,
) -> Connection:
    """
    Open a real psycopg connection using our database configuration.

    The caller owns the connection lifecycle. That means callers
    should normally use this function with a context manager:

        with connect_pipeline_database(config) as connection:
            ...

    A successful context-manager exit commits the transaction.
    An exception causes psycopg to roll it back.
    """

    return psycopg.connect(
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
    )