"""
Airflow DAG for the Open Food Facts raw ingestion pipeline.

Airflow owns scheduling and task execution.

The pipeline package owns extraction, validation, raw landing,
logical commit behavior, and watermark state.
"""

from datetime import timedelta
import os

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)
from pendulum import datetime

from airflow.providers.amazon.aws.notifications.sns import (
    send_sns_notification,
)
from pipeline.jobs.openfoodfacts_raw import (
    run_openfoodfacts_raw,
)
from pipeline.jobs.openfoodfacts_mart import (
    run_openfoodfacts_mart,
)
from pipeline.jobs.openfoodfacts_staging import (
    run_openfoodfacts_staging,
)

aws_account_id = os.environ["AWS_ACCOUNT_ID"]
aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
sns_topic_name = os.getenv(
    "SNS_ALERT_TOPIC_NAME",
    "portfolio-data-pipeline-alerts",
)

sns_topic_arn = (
    f"arn:aws:sns:{aws_region}:"
    f"{aws_account_id}:"
    f"{sns_topic_name}"
)


pipeline_failure_notification = send_sns_notification(
    aws_conn_id="aws_default",
    region_name=aws_region,
    target_arn=sns_topic_arn,
    subject=(
        "Airflow failure: "
        "{{ dag.dag_id }}"
    ),
    message=(
        "Portfolio Data Pipeline failure\n\n"
        "DAG: {{ dag.dag_id }}\n"
        "Task: {{ ti.task_id }}\n"
        "Run ID: {{ run_id }}\n"
        "Logical date: {{ logical_date }}\n"
        "Try number: {{ ti.try_number }}\n"
    ),
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

    # This source uses one forward-moving persistent watermark.
    #
    # Only one logical Open Food Facts run may execute at a time so
    # each run calculates its extraction window from the watermark
    # committed by the previous run.
    max_active_runs=1,

    tags=[
        "portfolio",
        "openfoodfacts",
        "raw",
    ],

    on_failure_callback=[
        pipeline_failure_notification,
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
        airflow_interval_end = context["data_interval_end"]

        # ----------------------------------------------------------
        # NORMALIZE TO OUR DAILY PIPELINE BOUNDARY
        # ----------------------------------------------------------
        #
        # Scheduled runs already end at midnight UTC because the DAG
        # runs on a daily midnight schedule.
        #
        # Manual DAG runs can receive a timetable-inferred interval
        # that does not necessarily end exactly at midnight.
        #
        # Our raw-layer run identity and watermark model are daily, so
        # normalize every Airflow invocation to the most recent completed
        # UTC midnight.
        batch_end = airflow_interval_end.in_timezone("UTC").replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        print(
            "Airflow data interval end: "
            f"{airflow_interval_end}"
        )

        print(
            "Pipeline batch end: "
            f"{batch_end}"
        )

        run_openfoodfacts_raw(
            batch_end=batch_end,
        )

    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def load_staging() -> None:
        """
        Load the committed raw batch into PostgreSQL staging.
        """

        context = get_current_context()

        airflow_interval_end = context["data_interval_end"]

        # Use the same normalized logical batch date as raw ingestion.
        batch_end = airflow_interval_end.in_timezone("UTC").replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        batch_date = batch_end.date()

        print(
            "Loading Open Food Facts staging batch: "
            f"{batch_date}"
        )

        run_openfoodfacts_staging(
            batch_date=batch_date,
        )
    @task(
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def load_mart() -> None:
        """
        Load one completed staging batch into the mart layer.

        The mart job handles:
        - Type 1 product-dimension merge
        - product-update fact loading
        - transactional commit / rollback
        """

        context = get_current_context()

        airflow_interval_end = context["data_interval_end"]

        # Use the same normalized logical batch date as both
        # raw ingestion and staging.
        batch_end = airflow_interval_end.in_timezone("UTC").replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

        batch_date = batch_end.date()

        print(
            "Loading Open Food Facts mart batch: "
            f"{batch_date}"
        )

        run_openfoodfacts_mart(
            batch_date=batch_date,
        )

    ingest_task = ingest_raw()
    staging_task = load_staging()
    mart_task = load_mart()

    ingest_task >> staging_task >> mart_task

openfoodfacts_raw()