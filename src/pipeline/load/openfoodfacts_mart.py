from datetime import date

from psycopg import Connection


def merge_product_dimension(
    connection: Connection,
    batch_date: date,
) -> int:
    """
    Merge one staging batch into mart.dim_product.

    Type 1 behavior:
    - new product code -> INSERT
    - existing product with newer source data -> UPDATE
    - existing product with same/older source data -> no change

    Returns the number of dimension rows inserted or updated.
    """

    merge_query = """
        MERGE INTO mart.dim_product AS target

        USING (
            /*
             * More than one staging row for the same product may
             * exist within a batch.
             *
             * Keep only the newest source version for each code.
             */
            SELECT DISTINCT ON (code)
                code,
                product_name,
                brands,
                nutrition_grades,
                categories_tags_en,
                last_modified_t
            FROM staging.openfoodfacts_products
            WHERE batch_date = %s
            ORDER BY
                code,
                last_modified_t DESC NULLS LAST
        ) AS source

        ON target.code = source.code

        WHEN MATCHED
            AND (
                target.source_last_modified_t IS NULL
                OR (
                    source.last_modified_t IS NOT NULL
                    AND source.last_modified_t >
                        target.source_last_modified_t
                )
            )
        THEN UPDATE SET
            product_name = source.product_name,
            brands = source.brands,
            nutrition_grades = source.nutrition_grades,
            categories_tags_en = source.categories_tags_en,
            source_last_modified_t = source.last_modified_t,
            updated_at = CURRENT_TIMESTAMP

        WHEN NOT MATCHED
        THEN INSERT (
            code,
            product_name,
            brands,
            nutrition_grades,
            categories_tags_en,
            source_last_modified_t
        )
        VALUES (
            source.code,
            source.product_name,
            source.brands,
            source.nutrition_grades,
            source.categories_tags_en,
            source.last_modified_t
        )
    """

    with connection.cursor() as cursor:
        cursor.execute(
            merge_query,
            (batch_date,),
        )

        return cursor.rowcount
    

def load_product_update_facts(
    connection: Connection,
    batch_date: date,
) -> int:
    """
    Load product-update events from one staging batch
    into mart.fact_product_update.

    The load is idempotent because the fact table enforces
    uniqueness on:

        product_key + source_last_modified_t

    A source event that appears again because of the extraction
    lookback window is therefore ignored.
    """

    insert_query = """
        INSERT INTO mart.fact_product_update (
            product_key,
            source_last_modified_t,
            batch_date
        )
        SELECT DISTINCT
            dim.product_key,
            staging.last_modified_t,
            staging.batch_date
        FROM staging.openfoodfacts_products AS staging

        INNER JOIN mart.dim_product AS dim
            ON dim.code = staging.code

        WHERE staging.batch_date = %s
          AND staging.last_modified_t IS NOT NULL

        ON CONFLICT (
            product_key,
            source_last_modified_t
        )
        DO NOTHING
    """

    with connection.cursor() as cursor:
        cursor.execute(
            insert_query,
            (batch_date,),
        )

        return cursor.rowcount