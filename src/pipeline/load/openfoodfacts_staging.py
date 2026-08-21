from datetime import date
from typing import Any

from psycopg import Connection


def replace_staging_batch(
    connection: Connection,
    rows: list[dict[str, Any]],
    batch_date: date,
    raw_object_key: str,
) -> int:
    """
    Replace one logical Open Food Facts staging batch.

    The load is idempotent at the batch level:

    1. Delete any rows already loaded for this batch/object.
    2. Insert the rows from the committed raw Parquet object.
    3. Perform both operations inside the caller's transaction.

    Returns the number of rows inserted.
    """

    delete_query = """
        DELETE FROM staging.openfoodfacts_products
        WHERE batch_date = %s
          AND raw_object_key = %s
    """

    insert_query = """
        INSERT INTO staging.openfoodfacts_products (
            code,
            product_name,
            brands,
            nutrition_grades,
            categories_tags_en,
            last_modified_t,
            batch_date,
            raw_object_key
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
    """

    with connection.cursor() as cursor:

        # Remove a previous load of this exact raw object.
        #
        # This makes a retry safe:
        #
        # first attempt:
        #     INSERT rows
        #
        # retry:
        #     DELETE same rows
        #     INSERT them again
        #
        # Final database state is the same.
        cursor.execute(
            delete_query,
            (
                batch_date,
                raw_object_key,
            ),
        )

        if not rows:
            return 0

        values = []

        for row in rows:
            values.append(
                (
                    row["code"],
                    row.get("product_name"),
                    row.get("brands"),
                    row.get("nutrition_grades"),
                    row.get("categories_tags_en"),
                    row.get("last_modified_t"),
                    batch_date,
                    raw_object_key,
                )
            )

        cursor.executemany(
            insert_query,
            values,
        )

    return len(rows)