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
from pipeline.storage.parquet import write_parquet
from pipeline.storage.s3 import S3StorageConfig, build_raw_object_key, create_s3_client, upload_file


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

    extract_date = datetime.now(UTC).date()

    total_products = 0

    with TemporaryDirectory(
        prefix="portfolio-data-pipeline-"
    ) as temp_dir:

        temp_directory = Path(temp_dir)

        for page_number, products in iter_product_pages(
            config=config,
            max_pages=2
        ):
            table = records_to_table(
                records=products,
                schema=OPENFOODFACTS_SCHEMA,
            )

            validate_openfoodfacts_table(table)

            local_path = (
                temp_directory
                / f"part-{page_number:05d}.parquet"
            )

            write_parquet(
                table=table,
                output_path=local_path,
            )

            object_key = build_raw_object_key(
                source="openfoodfacts",
                extract_date=extract_date,
                part_number=page_number,
            )

            s3_uri = upload_file(
                client=s3_client,
                local_path=local_path,
                config=s3_config,
                object_key=object_key,
            )

            total_products += table.num_rows

            print(
                f"Page {page_number}: "
                f"{table.num_rows} rows -> "
                f"{s3_uri}" 
            )

    print()
    print(
        f"Total products extracted: "
        f"{total_products}"
    )


if __name__ == "__main__":
    main()