import os
from datetime import date

from dotenv import load_dotenv

from pipeline.database import (
    connect_pipeline_database,
    load_pipeline_database_config,
)
from pipeline.load.openfoodfacts_staging import (
    replace_staging_batch,
)
from pipeline.state.raw_run import (
    build_success_key,
    create_raw_run_context,
)
from pipeline.storage.s3 import (
    S3StorageConfig,
    create_s3_client,
    get_json_object,
    get_object_bytes,
)
from pipeline.storage.parquet import (
    read_parquet_bytes,
)
from pipeline.storage.s3 import (
    S3StorageConfig,
    create_s3_client,
    get_json_object,
    get_object_bytes,
)

SOURCE_NAME = "openfoodfacts"


def get_committed_raw_object_keys(
    *,
    s3_client,
    s3_config: S3StorageConfig,
    batch_date: date,
) -> list[str]:
    """
    Return the Parquet object keys belonging to the committed raw run
    for one logical batch date.

    We deliberately discover the data through _SUCCESS.json rather
    than by listing the bucket.

    This guarantees that staging consumes only the attempt that
    successfully committed the logical raw run.
    """

    # Recreate the deterministic logical run identity for this date.
    #
    # create_raw_run_context() gives us:
    #   - the same deterministic run_id for source + logical_date
    #   - a new attempt_id
    #
    # build_success_key() only depends on source, logical_date,
    # and run_id, so the new attempt_id does not affect this key.
    run_context = create_raw_run_context(
        source=SOURCE_NAME,
        logical_date=batch_date,
    )

    success_key = build_success_key(
        run_context
    )

    # _SUCCESS.json is the durable indication that this raw batch
    # committed successfully.
    success_payload = get_json_object(
        client=s3_client,
        config=s3_config,
        object_key=success_key,
    )

    # The success marker points to the manifest belonging to the
    # physical attempt that actually won the commit.
    manifest_key = success_payload[
        "manifest_key"
    ]

    manifest_payload = get_json_object(
        client=s3_client,
        config=s3_config,
        object_key=manifest_key,
    )

    # Each manifest object has this structure:
    #
    # {
    #     "object_key": "...part-00001.parquet",
    #     "row_count": 50,
    #     "size_bytes": ...
    # }
    #
    # Staging only needs the object key.
    raw_object_keys = [
        obj["object_key"]
        for obj in manifest_payload["objects"]
    ]

    return raw_object_keys

def run_openfoodfacts_staging(
    *,
    batch_date: date,
) -> int:
    """
    Load one committed Open Food Facts raw batch into PostgreSQL staging.

    Returns the total number of staging rows inserted.
    """

    load_dotenv()

    # Load PostgreSQL configuration using the same shared helper
    # used by the raw extraction job.
    database_config = (
        load_pipeline_database_config()
    )

    s3_config = S3StorageConfig(
        bucket=os.environ["RAW_BUCKET"],
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv(
            "AWS_DEFAULT_REGION",
            "us-east-1",
        ),
    )

    s3_client = create_s3_client(
        s3_config
    )

    # Resolve the exact Parquet objects belonging to the
    # committed raw run for this logical batch.
    #
    # This follows:
    #
    # _SUCCESS.json
    #       ↓
    # manifest.json
    #       ↓
    # committed Parquet object keys
    raw_object_keys = get_committed_raw_object_keys(
        s3_client=s3_client,
        s3_config=s3_config,
        batch_date=batch_date,
    )

    total_rows = 0

    # One database transaction covers the whole logical staging batch.
    #
    # If any object fails to load, the transaction rolls back and
    # staging is left in its previous consistent state.
    with connect_pipeline_database(
        database_config
    ) as connection:

        for object_key in raw_object_keys:

            parquet_bytes = get_object_bytes(
                client=s3_client,
                config=s3_config,
                object_key=object_key,
            )

            table = read_parquet_bytes(
                parquet_bytes
            )

            rows = table.to_pylist()

            inserted_rows = replace_staging_batch(
                connection=connection,
                rows=rows,
                batch_date=batch_date,
                raw_object_key=object_key,
            )

            total_rows += inserted_rows

    print(
        f"Loaded {total_rows} Open Food Facts rows "
        f"into staging for batch {batch_date}."
    )

    return total_rows