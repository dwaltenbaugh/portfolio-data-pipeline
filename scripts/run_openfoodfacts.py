import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from dotenv import load_dotenv

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
from pipeline.state.raw_run import (
    RawObjectRecord,
    build_manifest_key,
    build_manifest_payload,
    build_part_key,
    build_success_key,
    build_success_payload,
    create_raw_run_context,
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