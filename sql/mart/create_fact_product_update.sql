CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.fact_product_update (
    product_update_key BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    -- Warehouse foreign key to the product dimension.
    product_key BIGINT NOT NULL
        REFERENCES mart.dim_product(product_key),

    -- Timestamp of the source-system modification event.
    source_last_modified_t BIGINT NOT NULL,

    -- Pipeline batch in which we first processed this event.
    batch_date DATE NOT NULL,

    -- Warehouse metadata.
    loaded_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    /*
     * Prevent the same source modification event from being
     * represented more than once.
     *
     * This is especially important because our extraction uses
     * a lookback window, so the same source record can legitimately
     * appear in more than one raw/staging batch.
     */
    UNIQUE (
        product_key,
        source_last_modified_t
    )
);