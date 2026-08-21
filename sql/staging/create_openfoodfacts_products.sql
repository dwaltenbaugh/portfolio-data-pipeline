CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.openfoodfacts_products (
    -- -------------------------------------------------------
    -- SOURCE FIELDS
    -- -------------------------------------------------------

    -- Open Food Facts product identifier / barcode.
    code TEXT NOT NULL,

    product_name TEXT,

    brands TEXT,

    nutrition_grades TEXT,

    -- The raw PyArrow schema stores this as list<string>.
    -- PostgreSQL's TEXT[] maps naturally to that structure.
    categories_tags_en TEXT[],

    -- Unix timestamp supplied by Open Food Facts.
    last_modified_t BIGINT,

    -- -------------------------------------------------------
    -- PIPELINE METADATA
    -- -------------------------------------------------------

    -- Logical raw batch that produced this staging row.
    -- This lets us identify and safely reload one day's data.
    batch_date DATE NOT NULL,

    -- Raw S3/MinIO object from which this row was loaded.
    -- Useful for lineage and debugging.
    raw_object_key TEXT NOT NULL,

    -- When the row entered the staging table.
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);