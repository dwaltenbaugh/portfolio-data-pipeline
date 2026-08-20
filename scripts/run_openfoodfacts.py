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
from pipeline.dq.openfoodfacts import (
    validate_openfoodfacts_table,
)
from pipeline.extract.openfoodfacts import (
    OpenFoodFactsConfig,
    iter_product_pages,
)
from pipeline.schemas.openfoodfacts import (
    OPENFOODFACTS_SCHEMA,
)
from pipeline.schemas.validation import (
    records_to_table,
)
from pipeline.state.extraction_window import (
    build_extraction_window,
)
from pipeline.state.raw_run import (
    RawObjectRecord,
    build_manifest_key,
    build_manifest_payload,
    build_part_key,
    build_success_key,
    build_success_payload,
    create_raw_run_context,
)
from pipeline.state.watermarks import (
    WatermarkRepository,
)
from pipeline.storage.parquet import write_parquet
from pipeline.storage.s3 import (
    ObjectAlreadyExistsError,
    S3StorageConfig,
    create_s3_client,
    object_exists,
    put_json_object,
    upload_file,
)


def main() -> None:
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    # ----------------------------------------------------------
    # PIPELINE DATABASE CONFIGURATION
    # ----------------------------------------------------------
    #
    # Load the PostgreSQL connection settings that were placed in
    # .env. This database stores operational pipeline state such as
    # source watermarks.
    database_config = (
        load_pipeline_database_config()
    )

    # ----------------------------------------------------------
    # OPEN FOOD FACTS INCREMENTAL CONFIGURATION
    # ----------------------------------------------------------

    # Parse the configured starting timestamp for this source.
    #
    # datetime.fromisoformat() turns a string such as:
    #
    #   2026-08-01T00:00:00+00:00
    #
    # into a Python datetime object.
    #
    # The +00:00 portion is important because it makes the datetime
    # timezone-aware.
    initial_watermark = datetime.fromisoformat(
        os.environ[
            "OPENFOODFACTS_INITIAL_WATERMARK"
        ]
    )


    # Environment variables are strings, so convert the configured
    # number of lookback hours into an integer and then into a
    # timedelta that our window builder understands.
    lookback = timedelta(
        hours=int(
            os.environ.get(
                "OPENFOODFACTS_LOOKBACK_HOURS",
                "0",
            )
        )
    )


    # ----------------------------------------------------------
    # DETERMINE THIS RUN'S UPPER TIME BOUNDARY
    # ----------------------------------------------------------

    # Capture "now" once so every calculation in this block uses
    # the same point in time.
    now_utc = datetime.now(UTC)


    # This is a DAILY batch pipeline, so we only process through
    # the most recently completed UTC day boundary.
    #
    # For example, if the script runs at:
    #
    #   2026-08-20 14:30 UTC
    #
    # batch_end becomes:
    #
    #   2026-08-20 00:00 UTC
    #
    # We do NOT use 14:30 because that would create a partial-day
    # batch whose boundary changes depending on execution time.
    batch_end = now_utc.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    # ----------------------------------------------------------
    # READ CURRENT PIPELINE STATE
    # ----------------------------------------------------------

    # Open a real PostgreSQL connection.
    #
    # We only READ state in this milestone. No watermark will be
    # advanced yet.
    with connect_pipeline_database(
        database_config
    ) as connection:

        # The repository hides the SQL details from the runner.
        watermark_repository = WatermarkRepository(
            connection=connection
        )

        # Ask PostgreSQL:
        #
        # "What timestamp has Open Food Facts been successfully
        # processed through?"
        #
        # Because we have not inserted a real OpenFoodFacts
        # watermark yet, the first execution should return None.
        current_watermark = (
            watermark_repository.get_watermark(
                "openfoodfacts"
            )
        )

    # ----------------------------------------------------------
    # CALCULATE THIS RUN'S EXTRACTION WINDOW
    # ----------------------------------------------------------
    #
    # This combines:
    #
    #   stored PostgreSQL state
    #       +
    #   source configuration
    #       +
    #   today's stable batch boundary
    #       +
    #   late-arriving-data lookback
    #
    # into one immutable ExtractionWindow object.
    extraction_window = build_extraction_window(
        source_name="openfoodfacts",
        current_watermark=current_watermark,
        initial_watermark=initial_watermark,
        batch_end=batch_end,
        lookback=lookback,
    )

    # ----------------------------------------------------------
    # TEMPORARY OBSERVABILITY
    # ----------------------------------------------------------
    #
    # Print the calculated window before doing any extraction.
    #
    # For now this is intentionally verbose so we can visually
    # inspect the state calculation while developing the pipeline.
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


    user_agent = os.environ["OPENFOODFACTS_USER_AGENT"]

    config = OpenFoodFactsConfig(
        user_agent=user_agent,
        page_size=50,
    )

    s3_config = S3StorageConfig(
        bucket=os.environ["RAW_BUCKET"],
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        region_name=os.getenv(
            "AWS_DEFAULT_REGION",
            "us-east-1",
        )
    )

    s3_client = create_s3_client(s3_config)

    # Use today's UTC date as the logical extraction date.
    #
    # Later, when Airflow controls execution, we'll get the logical
    # date from Airflow instead of relying directly on datetime.now().
    extract_date = datetime.now(UTC).date()

    # Create the identity for this raw extraction.
    #
    # The run_id is deterministic:
    #   same source + same logical date = same run_id
    #
    # The attempt_id is unique:
    #   every actual execution gets a new attempt_id
    #
    # This lets us distinguish:
    #   "Which batch are we processing?"
    # from:
    #   "Which attempt at processing that batch is this?"
    run_context = create_raw_run_context(
        source="openfoodfacts",
        logical_date=extract_date,
    )

    # Build the key for the logical run's final commit marker.
    #
    # Notice that _SUCCESS.json is NOT underneath attempt_id.
    # It belongs to the logical run as a whole.
    #
    # Example:
    #
    # openfoodfacts/
    #   extract_date=2026-08-19/
    #   run_id=ABC123/
    #   _SUCCESS.json
    success_key = build_success_key(
        run_context
    )

    # Before doing any API work, check whether this logical run
    # has already completed successfully.
    #
    # If _SUCCESS.json already exists, there is nothing to redo.
    if object_exists(
        client=s3_client,
        config=s3_config,
        object_key=success_key,
    ):
        print(
            "Raw run is already committed. "
            "Nothing to do."
        )
        return

    # Keep an in-memory record of every Parquet object that
    # successfully lands in raw storage during this attempt.
    #
    # Later we'll use this list to create manifest.json.
    landed_objects: list[RawObjectRecord] = []

    # TemporaryDirectory gives us short-lived disk space for
    # converting Arrow tables into Parquet before uploading them.
    #
    # Python automatically deletes this directory when the
    # with-block finishes.
    with TemporaryDirectory(
        prefix="portfolio-data-pipeline-"
    ) as temp_dir:

        temp_directory = Path(temp_dir)

        # iter_product_pages() yields one API page at a time so we
        # don't accumulate the entire API response in memory.
        for page_number, products in iter_product_pages(
            config=config,
            max_pages=2,
        ):
            # Convert the API records into an Arrow table using our
            # explicit Open Food Facts schema.
            table = records_to_table(
                records=products,
                schema=OPENFOODFACTS_SCHEMA,
            )

            # Run blocking raw-layer DQ checks before we allow this
            # chunk to enter durable raw storage.
            validate_openfoodfacts_table(table)

            # Create a temporary local filename for this chunk.
            local_path = (
                temp_directory
                / f"part-{page_number:05d}.parquet"
            )

            # Serialize the validated Arrow table into Parquet.
            write_parquet(
                table=table,
                output_path=local_path,
            )

            # Build this part's durable raw-storage key.
            #
            # The key includes the logical run and execution attempt.
            object_key = build_part_key(
                context=run_context,
                part_number=page_number,
            )

            # Upload the temporary Parquet file to MinIO/S3.
            #
            # upload_file() also calls head_object() afterward to
            # verify that the uploaded object can be found.
            s3_uri = upload_file(
                client=s3_client,
                local_path=local_path,
                config=s3_config,
                object_key=object_key,
            )

            # Only after the upload succeeds do we record this part
            # as successfully landed.
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

    # At this point, every API page finished successfully and every
    # Parquet part was uploaded successfully.
    #
    # Now we can create a manifest that describes exactly what this
    # execution attempt produced.
    completed_at = datetime.now(UTC)

    manifest_key = build_manifest_key(
        run_context
    )

    manifest_payload = build_manifest_payload(
        context=run_context,
        objects=landed_objects,
        completed_at=completed_at,
    )

    # Write the manifest for this specific execution attempt.
    #
    # Unlike _SUCCESS.json, the manifest lives under attempt_id,
    # so each attempt gets its own manifest.
    manifest_uri = put_json_object(
        client=s3_client,
        config=s3_config,
        object_key=manifest_key,
        payload=manifest_payload,
    )

    print(
        f"Manifest written -> {manifest_uri}"
    )

    # Build the small JSON document that says:
    #
    # "This logical raw run is complete, and THIS attempt is
    # the committed attempt that downstream processing should use."
    committed_at = datetime.now(UTC)

    success_payload = build_success_payload(
        context=run_context,
        manifest_key=manifest_key,
        objects=landed_objects,
        committed_at=committed_at,
    )

    try:
        # Write the final logical-run commit marker.
        #
        # if_absent=True is important:
        # it means this object may only be created if _SUCCESS.json
        # does not already exist.
        success_uri = put_json_object(
            client=s3_client,
            config=s3_config,
            object_key=success_key,
            payload=success_payload,
            if_absent=True,
        )

    except ObjectAlreadyExistsError:
        # Another attempt already committed this same logical run.
        #
        # We do not overwrite its success marker.
        print(
            "Another attempt already committed "
            "this logical run."
        )
        return

    print(
    f"Run committed -> {success_uri}"
    )

    # Calculate the total number of rows that actually made it
    # into durable raw storage during this execution attempt.
    #
    # We derive this from landed_objects instead of maintaining a
    # separate counter, so our total reflects only successful uploads.
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