CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Natural/business key from Open Food Facts.
    code TEXT NOT NULL UNIQUE,

    -- Descriptive product attributes.
    product_name TEXT,
    brands TEXT,
    nutrition_grades TEXT,
    categories_tags_en TEXT[],

    -- Latest source modification timestamp we have processed
    -- for this product.
    source_last_modified_t BIGINT,

    -- Warehouse metadata.
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);