import logging
import os
from datetime import UTC, datetime
from pathlib import Path

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

    extract_date = datetime.now(UTC).date()

    total_products = 0

    for page_number, products in iter_product_pages(
        config=config,
        max_pages=2
    ):
        table = records_to_table(
            records=products,
            schema=OPENFOODFACTS_SCHEMA,
        )

        validate_openfoodfacts_table(table)

        output_path = Path(
            "data/local/raw/"
            f"openfoodfacts/"
            f"extract_date={extract_date}/"
            f"part-{page_number:05d}.parquet"
        )

        write_parquet(
            table=table,
            output_path=output_path,
        )

        total_products += table.num_rows

        print(
            f"Page {page_number}: "
            f"{table.num_rows} rows -> "
            f"{output_path}" 
        )

        print()
        print(f"Total products extracted: {total_products}")


if __name__ == "__main__":
    main()