from datetime import date

import pyarrow as pa
import pyarrow.compute as pc
from psycopg import Connection

from pipeline.dq.metrics import record_dq_metric


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

     # Persist the observed staging row count.
    record_dq_metric(
        connection=connection,
        source_name="openfoodfacts",
        pipeline_layer="staging",
        batch_date=batch_date,
        metric_name="row_count",
        metric_value=row_count,
        passed=(row_count == expected_row_count),
        expected_value=expected_row_count,
    )

    # Persist the null-code check.
    record_dq_metric(
        connection=connection,
        source_name="openfoodfacts",
        pipeline_layer="staging",
        batch_date=batch_date,
        metric_name="null_code_count",
        metric_value=null_code_count,
        passed=(null_code_count == 0),
        expected_value=0,
    )

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


def validate_openfoodfacts_mart_batch(
    connection: Connection,
    batch_date: date,
) -> None:
    """
    Run blocking mart-layer checks for one processed batch.

    Checks:
    - every staging product code exists in dim_product
    - every non-null staging update event exists in fact_product_update
    """

    query = """
        SELECT
            /*
             * Count staging rows whose business key cannot be found
             * in the product dimension.
             */
            COUNT(*) FILTER (
                WHERE dim.product_key IS NULL
            ) AS missing_dimension_count,

            /*
             * Count staging update events that cannot be found
             * in the fact table.
             */
            COUNT(*) FILTER (
                WHERE staging.last_modified_t IS NOT NULL
                  AND fact.product_update_key IS NULL
            ) AS missing_fact_count

        FROM staging.openfoodfacts_products AS staging

        LEFT JOIN mart.dim_product AS dim
            ON dim.code = staging.code

        LEFT JOIN mart.fact_product_update AS fact
            ON fact.product_key = dim.product_key
           AND fact.source_last_modified_t =
               staging.last_modified_t

        WHERE staging.batch_date = %s
    """

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (batch_date,),
        )

        result = cursor.fetchone()

    if result is None:
        raise DataQualityError(
            "Unable to retrieve mart DQ metrics."
        )

    missing_dimension_count = result[0]
    missing_fact_count = result[1]

    record_dq_metric(
        connection=connection,
        source_name="openfoodfacts",
        pipeline_layer="mart",
        batch_date=batch_date,
        metric_name="missing_dimension_count",
        metric_value=missing_dimension_count,
        passed=(missing_dimension_count == 0),
        expected_value=0,
    )

    record_dq_metric(
        connection=connection,
        source_name="openfoodfacts",
        pipeline_layer="mart",
        batch_date=batch_date,
        metric_name="missing_fact_count",
        metric_value=missing_fact_count,
        passed=(missing_fact_count == 0),
        expected_value=0,
    )

    if missing_dimension_count > 0:
        raise DataQualityError(
            "Mart dimension reconciliation failed: "
            f"{missing_dimension_count} staging rows "
            "have no matching dim_product row."
        )

    if missing_fact_count > 0:
        raise DataQualityError(
            "Mart fact reconciliation failed: "
            f"{missing_fact_count} staging update events "
            "have no matching fact_product_update row."
        )