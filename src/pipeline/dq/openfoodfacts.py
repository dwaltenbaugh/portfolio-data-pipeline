import pyarrow as pa
import pyarrow.compute as pc
from datetime import date
from psycopg import Connection

from pipeline.storage.s3 import (
    S3StorageConfig,
    create_s3_client,
    get_json_object,
    get_object_bytes,
)


class DataQualityError(Exception):
    """Raised when data fails a blocking quality check."""


def validate_openfoodfacts_table(
        table: pa.Table,
) -> None:
    """Run blocking raw-layer checks on Open Food Facts data."""

    if table.num_rows == 0:
        raise DataQualityError(
            "Open Food Facts contains zero rows."
        )

    null_code_count = pc.sum(
        pc.is_null(table["code"])
    ).as_py()

    if null_code_count > 0:
        raise DataQualityError(
            f"Found {null_code_count} records with null product codes."
        )

    unique_code_count = pc.count_distinct(
        table["code"]
    ).as_py()

    if unique_code_count != table.num_rows:
        raise DataQualityError(
            "Duplicate product codes detected within page."
        )


def validate_openfoodfacts_staging_batch(
    connection: Connection,
    batch_date: date,
    expected_row_count: int,
) -> None:
    """
    Run blocking checks against one loaded staging batch.

    The staging batch must contain exactly the number of rows
    we expected to load from the committed raw manifest.
    """

    query = """
        SELECT
            COUNT(*) AS row_count,
            COUNT(*) FILTER (
                WHERE code IS NULL
            ) AS null_code_count
        FROM staging.openfoodfacts_products
        WHERE batch_date = %s
    """

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (batch_date,),
        )

        result = cursor.fetchone()

    if result is None:
        raise DataQualityError(
            "Unable to retrieve staging DQ metrics."
        )

    row_count = result[0]
    null_code_count = result[1]

    if row_count != expected_row_count:
        raise DataQualityError(
            "Staging row-count mismatch: "
            f"expected {expected_row_count}, "
            f"found {row_count}."
        )

    if null_code_count > 0:
        raise DataQualityError(
            "Staging contains "
            f"{null_code_count} rows with null product codes."
        )