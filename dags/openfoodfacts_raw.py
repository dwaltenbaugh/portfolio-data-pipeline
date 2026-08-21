"""
Airflow DAG for the Open Food Facts raw ingestion pipeline.

Airflow owns scheduling and task execution.

The pipeline package owns extraction, validation, raw landing,
logical commit behavior, and watermark state.
"""

from datetime import timedelta

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)
from pendulum import datetime

from pipeline.jobs.openfoodfacts_raw import (
    run_openfoodfacts_raw,
)


@dag(
    dag_id="openfoodfacts_raw",

    # Run once per day at midnight UTC.
    schedule="0 0 * * *",

    # The first schedulable boundary for this DAG.
    start_date=datetime(
        2026,
        8,
        1,
        tz="UTC",
    ),

    # We do not want Airflow automatically generating historical
    # runs all the way back to start_date during development.
    catchup=False,

    tags=[
        "portfolio",
        "openfoodfacts",
        "raw",
    ],
)
def openfoodfacts_raw():
    """
    Orchestrate one logical Open Food Facts raw ingestion run.
    """

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def ingest_raw() -> None:
        """
        Execute the idempotent raw ingestion unit.

        The application layer already handles:
        - extraction-window calculation
        - API pagination
        - schema enforcement
        - data-quality gates
        - Parquet serialization
        - S3/MinIO landing
        - manifest creation
        - _SUCCESS commit
        - PostgreSQL watermark reconciliation
        """

        context = get_current_context()

        # For a scheduled DAG run, Airflow's data_interval_end is
        # the logical upper boundary of the period being processed.
        #
        # Passing it into the job makes execution reproducible:
        # retries and re-runs use the same logical boundary rather
        # than whatever datetime.now() happens to return.
        batch_end = context["data_interval_end"]

        run_openfoodfacts_raw(
            batch_end=batch_end,
        )

    ingest_raw()


openfoodfacts_raw()