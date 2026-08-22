from datetime import date

from dotenv import load_dotenv

from pipeline.database import (
    connect_pipeline_database,
    load_pipeline_database_config,
)
from pipeline.dq.openfoodfacts import (
    validate_openfoodfacts_mart_batch,
)
from pipeline.load.openfoodfacts_mart import (
    load_product_update_facts,
    merge_product_dimension,
)


def run_openfoodfacts_mart(
    *,
    batch_date: date,
) -> tuple[int, int]:
    """
    Load one Open Food Facts staging batch into the mart layer.

    Processing order matters:

        1. Merge dim_product
        2. Load fact_product_update

    The fact load depends on product_key values in dim_product.

    Both operations run inside one database transaction.

    Returns:
        (
            dimension_rows_changed,
            fact_rows_inserted,
        )
    """

    load_dotenv()

    database_config = (
        load_pipeline_database_config()
    )

    # One transaction covers the whole mart load.
    #
    # If either the dimension merge or fact load fails,
    # psycopg rolls back both operations.
    with connect_pipeline_database(
        database_config
    ) as connection:

        dimension_rows_changed = (
            merge_product_dimension(
                connection=connection,
                batch_date=batch_date,
            )
        )

        fact_rows_inserted = (
            load_product_update_facts(
                connection=connection,
                batch_date=batch_date,
            )
        )

        # Run mart DQ before the transaction commits.
        #
        # If validation raises DataQualityError, leaving the
        # connection context rolls back the entire mart load.
        validate_openfoodfacts_mart_batch(
            connection=connection,
            batch_date=batch_date,
        )

    print(
        "Open Food Facts mart load complete for "
        f"{batch_date}: "
        f"{dimension_rows_changed} dimension rows changed, "
        f"{fact_rows_inserted} fact rows inserted."
    )

    return (
        dimension_rows_changed,
        fact_rows_inserted,
    )