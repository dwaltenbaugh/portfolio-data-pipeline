import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from dotenv import load_dotenv

from pipeline.database import (
    connect_pipeline_database,
    load_pipeline_database_config,
)
from pipeline.dq.openfoodfacts import validate_openfoodfacts_table
from pipeline.extract.openfoodfacts import (
    OpenFoodFactsConfig,
    iter_product_pages,
)
from pipeline.schemas.openfoodfacts import OPENFOODFACTS_SCHEMA
from pipeline.schemas.validation import records_to_table
from pipeline.state.extraction_window import (
    ExtractionWindow,
    build_extraction_window,
)
from pipeline.state.raw_run import (
    RawObjectRecord,
    RawRunContext,
    build_manifest_key,
    build_manifest_payload,
    build_part_key,
    build_success_key,
    build_success_payload,
    create_raw_run_context,
)
from pipeline.state.watermarks import (
    WatermarkRepository,
    reconcile_watermark,
)
from pipeline.storage.parquet import write_parquet
from pipeline.storage.s3 import (
    ObjectAlreadyExistsError,
    S3StorageConfig,
    create_s3_client,
    get_json_object,
    object_exists,
    put_json_object,
    upload_file,
)

SOURCE_NAME = "openfoodfacts"


def reconcile_success_marker(
    *,
    s3_client,
    s3_config: S3StorageConfig,
    success_key: str,
    database_config,
) -> None:
    """
    Reconcile PostgreSQL with an already-committed raw run.

    This is our recovery path for this failure sequence:

        Parquet succeeds
            ↓
        manifest.json succeeds
            ↓
        _SUCCESS.json succeeds
            ↓
        process crashes
            ↓
        PostgreSQL watermark never advances

    A retry reads _SUCCESS.json and finishes the database update
    without extracting the source data again.
    """

    # Read the durable raw-layer commit record.
    success_payload = get_json_object(
        client=s3_client,
        config=s3_config,
        object_key=success_key,
    )

    # JSON stores datetime values as strings, so convert the
    # committed watermark back into a Python datetime.
    committed_watermark = datetime.fromisoformat(
        success_payload["next_watermark"]
    )

    source_name = success_payload["source"]
    committed_run_id = success_payload["run_id"]

    # Reconcile PostgreSQL inside a transaction.
    #
    # Successful exit commits.
    # An exception rolls the transaction back.
    with connect_pipeline_database(
        database_config
    ) as connection:
        repository = WatermarkRepository(
            connection=connection
        )

        changed = reconcile_watermark(
            repository=repository,
            source_name=source_name,
            committed_watermark=committed_watermark,
            run_id=committed_run_id,
        )

    if changed:
        print(
            "Raw run was already committed; "
            "PostgreSQL watermark was reconciled."
        )
    else:
        print(
            "Raw run and PostgreSQL watermark "
            "are already synchronized."
        )


def read_current_watermark(
    *,
    database_config,
    source_name: str,
) -> datetime | None:
    """
    Read the most recently committed high-water mark for a source.

    None means the source has never successfully advanced its
    watermark.
    """

    with connect_pipeline_database(
        database_config
    ) as connection:
        repository = WatermarkRepository(
            connection=connection
        )

        return repository.get_watermark(
            source_name
        )


def build_source_extraction_window(
    *,
    database_config,
    batch_end: datetime,
) -> ExtractionWindow:
    """
    Read persistent watermark state and calculate the source window.

    This function does NOT mutate the watermark.
    """

    current_watermark = read_current_watermark(
        database_config=database_config,
        source_name=SOURCE_NAME,
    )

    # The initial watermark is source configuration.
    # It defines the earliest timestamp our pipeline will process.
    initial_watermark = datetime.fromisoformat(
        os.environ[
            "OPENFOODFACTS_INITIAL_WATERMARK"
        ]
    )

    # Re-read a small period before the high-water mark so that
    # late-arriving or recently updated records can be captured.
    lookback = timedelta(
        hours=int(
            os.environ.get(
                "OPENFOODFACTS_LOOKBACK_HOURS",
                "0",
            )
        )
    )

    return build_extraction_window(
        source_name=SOURCE_NAME,
        current_watermark=current_watermark,
        initial_watermark=initial_watermark,
        batch_end=batch_end,
        lookback=lookback,
    )


def print_extraction_window(
    extraction_window: ExtractionWindow,
) -> None:
    """
    Print the calculated boundaries while the pipeline is under
    development.

    Later we can replace this with structured logging/metrics.
    """

    print()
    print("Open Food Facts extraction window")
    print("---------------------------------")
    print(
        "Previous watermark: "
        f"{extraction_window.previous_watermark}"
    )
    print(
        "Incremental start:  "
        f"{extraction_window.incremental_start}"
    )
    print(
        "Extract start:      "
        f"{extraction_window.extract_start}"
    )
    print(
        "Extract end:        "
        f"{extraction_window.extract_end}"
    )
    print(
        "Next watermark:     "
        f"{extraction_window.next_watermark}"
    )
    print()


def extract_and_land_parts(
    *,
    extractor_config: OpenFoodFactsConfig,
    extraction_window: ExtractionWindow,
    s3_client,
    s3_config: S3StorageConfig,
    run_context: RawRunContext,
) -> list[RawObjectRecord]:
    """
    Extract API pages that fall inside this run's extraction window,
    validate them, serialize them to Parquet, and upload each
    successfully validated part to raw storage.

    The extraction window is passed all the way down to the source
    extractor so that records outside:

        extract_start <= last_modified_t < extract_end

    are not included in the raw files produced by this run.

    Returns metadata describing only the objects that successfully
    landed in MinIO/S3.
    """

    # OPENFOODFACTS_MAX_PAGES is an optional operational guard.
    #
    # Leaving it unset means pagination is driven entirely by the
    # extraction window.
    #
    # Setting it can be useful during local development or when we
    # deliberately want to limit calls to a third-party API.
    max_pages_value = os.getenv(
        "OPENFOODFACTS_MAX_PAGES"
    )

    max_pages = (
        int(max_pages_value)
        if max_pages_value
        else None
    )

    landed_objects: list[RawObjectRecord] = []

    # Local disk is only temporary working storage.
    #
    # MinIO/S3 is the durable raw layer.
    with TemporaryDirectory(
        prefix="portfolio-data-pipeline-"
    ) as temp_dir:
        temp_directory = Path(temp_dir)

        # Yield one API page at a time so we don't accumulate the
        # entire source response in memory.
        for page_number, products in iter_product_pages(
            config=extractor_config,

            # Only these two boundaries matter to the source extractor.
            #
            # We intentionally do NOT pass previous_watermark or
            # next_watermark here. Those are pipeline-state concepts,
            # not API-extraction concepts.
            extract_start=extraction_window.extract_start,
            extract_end=extraction_window.extract_end,

            max_pages=10,
        ):
            # Enforce the expected raw schema.
            table = records_to_table(
                records=products,
                schema=OPENFOODFACTS_SCHEMA,
            )

            # Reject the chunk before durable storage if blocking
            # data-quality checks fail.
            validate_openfoodfacts_table(table)

            local_path = (
                temp_directory
                / f"part-{page_number:05d}.parquet"
            )

            # Serialize the validated Arrow table to Parquet.
            write_parquet(
                table=table,
                output_path=local_path,
            )

            # Each part belongs to this specific physical attempt.
            object_key = build_part_key(
                context=run_context,
                part_number=page_number,
            )

            # Upload the temporary Parquet file to durable raw storage.
            s3_uri = upload_file(
                client=s3_client,
                local_path=local_path,
                config=s3_config,
                object_key=object_key,
            )

            # Only record metadata AFTER the upload succeeds.
            #
            # This ensures our eventual manifest describes only
            # objects that actually landed.
            landed_objects.append(
                RawObjectRecord(
                    object_key=object_key,
                    row_count=table.num_rows,
                    size_bytes=local_path.stat().st_size,
                )
            )

            print(
                f"Page {page_number}: "
                f"{table.num_rows} rows -> "
                f"{s3_uri}"
            )

    return landed_objects


def write_manifest(
    *,
    s3_client,
    s3_config: S3StorageConfig,
    run_context: RawRunContext,
    landed_objects: list[RawObjectRecord],
) -> str:
    """
    Write the attempt-specific manifest after every Parquet part
    has landed successfully.

    The manifest is an inventory of the raw objects produced by
    this execution attempt.
    """

    completed_at = datetime.now(UTC)

    manifest_key = build_manifest_key(
        run_context
    )

    manifest_payload = build_manifest_payload(
        context=run_context,
        objects=landed_objects,
        completed_at=completed_at,
    )

    manifest_uri = put_json_object(
        client=s3_client,
        config=s3_config,
        object_key=manifest_key,
        payload=manifest_payload,
    )

    print(
        f"Manifest written -> {manifest_uri}"
    )

    # The success payload needs the key, not the URI.
    return manifest_key


def commit_raw_run(
    *,
    s3_client,
    s3_config: S3StorageConfig,
    run_context: RawRunContext,
    extraction_window: ExtractionWindow,
    landed_objects: list[RawObjectRecord],
    manifest_key: str,
    database_config,
) -> bool:
    """
    Commit the logical raw run by creating _SUCCESS.json.

    Returns True if this execution created the success marker.

    Returns False if another attempt had already committed the
    logical run. In that case, this function also reconciles
    PostgreSQL with the winning attempt.
    """

    success_key = build_success_key(
        run_context
    )

    # Capture the approximate logical-commit timestamp immediately
    # before attempting to create _SUCCESS.json.
    committed_at = datetime.now(UTC)

    success_payload = build_success_payload(
        context=run_context,
        manifest_key=manifest_key,
        objects=landed_objects,
        committed_at=committed_at,
        extract_start=extraction_window.extract_start,
        extract_end=extraction_window.extract_end,
        next_watermark=extraction_window.next_watermark,
    )

    try:
        # if_absent=True maps to a conditional S3 PUT.
        #
        # Only one attempt may create this logical run's canonical
        # _SUCCESS.json.
        success_uri = put_json_object(
            client=s3_client,
            config=s3_config,
            object_key=success_key,
            payload=success_payload,
            if_absent=True,
        )

        print(
            f"Run committed -> {success_uri}"
        )

        return True

    except ObjectAlreadyExistsError:
        # Another attempt won the commit race.
        #
        # Respect its _SUCCESS marker and ensure PostgreSQL reflects
        # the committed watermark.
        print(
            "Another attempt already committed "
            "this logical run."
        )

        reconcile_success_marker(
            s3_client=s3_client,
            s3_config=s3_config,
            success_key=success_key,
            database_config=database_config,
        )

        return False


def advance_committed_watermark(
    *,
    database_config,
    extraction_window: ExtractionWindow,
    run_context: RawRunContext,
) -> None:
    """
    Advance PostgreSQL only after raw storage has successfully
    committed the logical run.

    This ordering is a critical pipeline invariant:

        raw commit first
            ↓
        watermark second
    """

    with connect_pipeline_database(
        database_config
    ) as connection:
        repository = WatermarkRepository(
            connection=connection
        )

        changed = reconcile_watermark(
            repository=repository,
            source_name=SOURCE_NAME,
            committed_watermark=(
                extraction_window.next_watermark
            ),
            run_id=run_context.run_id,
        )

    if changed:
        print(
            "PostgreSQL watermark advanced -> "
            f"{extraction_window.next_watermark}"
        )
    else:
        print(
            "PostgreSQL watermark was already current."
        )


def main() -> None:
    """
    Run one logical Open Food Facts raw extraction.

    High-level flow:

        configuration
            ↓
        logical run identity
            ↓
        recovery/idempotency check
            ↓
        extraction window
            ↓
        API → Arrow → DQ → Parquet → S3
            ↓
        manifest
            ↓
        _SUCCESS
            ↓
        watermark advancement
    """

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s "
            "%(name)s - %(message)s"
        ),
    )

    # ----------------------------------------------------------
    # 1. LOAD SHARED INFRASTRUCTURE CONFIGURATION
    # ----------------------------------------------------------

    database_config = (
        load_pipeline_database_config()
    )

    s3_config = S3StorageConfig(
        bucket=os.environ["RAW_BUCKET"],
        endpoint_url=os.getenv(
            "S3_ENDPOINT_URL"
        ),
        region_name=os.getenv(
            "AWS_DEFAULT_REGION",
            "us-east-1",
        ),
    )

    s3_client = create_s3_client(
        s3_config
    )

    # ----------------------------------------------------------
    # 2. IDENTIFY THIS LOGICAL DAILY RUN
    # ----------------------------------------------------------

    # Capture the clock once.
    now_utc = datetime.now(UTC)

    # Daily batches use the most recent completed UTC boundary.
    #
    # Example:
    #   current time = Aug 20 17:30
    #   batch_end    = Aug 20 00:00
    batch_end = now_utc.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    extract_date = batch_end.date()

    # Create this ONCE.
    #
    # run_id is stable for the logical source/date.
    # attempt_id is unique for this physical execution.
    run_context = create_raw_run_context(
        source=SOURCE_NAME,
        logical_date=extract_date,
    )

    success_key = build_success_key(
        run_context
    )

    # ----------------------------------------------------------
    # 3. RECOVER OR SHORT-CIRCUIT AN EXISTING COMMIT
    # ----------------------------------------------------------

    # This check deliberately happens BEFORE building another
    # extraction window.
    #
    # If today's logical raw run already committed, today's
    # watermark may already equal batch_end. That is not an error;
    # it simply means there is no new work to extract.
    if object_exists(
        client=s3_client,
        config=s3_config,
        object_key=success_key,
    ):
        reconcile_success_marker(
            s3_client=s3_client,
            s3_config=s3_config,
            success_key=success_key,
            database_config=database_config,
        )

        return

    # ----------------------------------------------------------
    # 4. CALCULATE THE INCREMENTAL EXTRACTION WINDOW
    # ----------------------------------------------------------

    extraction_window = build_source_extraction_window(
        database_config=database_config,
        batch_end=batch_end,
    )

    print_extraction_window(
        extraction_window
    )

    # ----------------------------------------------------------
    # 5. CONFIGURE THE SOURCE EXTRACTOR
    # ----------------------------------------------------------

    extractor_config = OpenFoodFactsConfig(
        user_agent=os.environ[
            "OPENFOODFACTS_USER_AGENT"
        ],
        page_size=50,
    )

    # ----------------------------------------------------------
    # 6. EXTRACT, VALIDATE, AND LAND RAW PARTS
    # ----------------------------------------------------------

    landed_objects = extract_and_land_parts(
    extractor_config=extractor_config,

    # The source extractor now needs the calculated boundaries,
    # not merely the infrastructure configuration.
    extraction_window=extraction_window,

    s3_client=s3_client,
    s3_config=s3_config,
    run_context=run_context,
)

    # ----------------------------------------------------------
    # 7. WRITE THE ATTEMPT MANIFEST
    # ----------------------------------------------------------

    manifest_key = write_manifest(
        s3_client=s3_client,
        s3_config=s3_config,
        run_context=run_context,
        landed_objects=landed_objects,
    )

    # ----------------------------------------------------------
    # 8. COMMIT THE LOGICAL RAW RUN
    # ----------------------------------------------------------

    committed_by_this_attempt = commit_raw_run(
        s3_client=s3_client,
        s3_config=s3_config,
        run_context=run_context,
        extraction_window=extraction_window,
        landed_objects=landed_objects,
        manifest_key=manifest_key,
        database_config=database_config,
    )

    # If another attempt already committed, commit_raw_run()
    # reconciled PostgreSQL using the winning _SUCCESS marker.
    # There is nothing more for this attempt to do.
    if not committed_by_this_attempt:
        return

    # ----------------------------------------------------------
    # 9. ADVANCE PERSISTENT WATERMARK STATE
    # ----------------------------------------------------------

    advance_committed_watermark(
        database_config=database_config,
        extraction_window=extraction_window,
        run_context=run_context,
    )

    # ----------------------------------------------------------
    # 10. REPORT SUCCESSFUL RAW-LAYER ROW COUNT
    # ----------------------------------------------------------

    total_products = sum(
        obj.row_count
        for obj in landed_objects
    )

    print()
    print(
        f"Total products extracted: "
        f"{total_products}"
    )


if __name__ == "__main__":
    main()