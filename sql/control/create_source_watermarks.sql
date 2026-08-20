-- The control schema contains operational metadata used by the
-- pipeline itself. It is separate from staging and analytical data.
CREATE SCHEMA IF NOT EXISTS control;

-- Store the last successfully committed extraction position
-- independently for every source.
--
-- One row represents one source's current pipeline state.
CREATE TABLE IF NOT EXISTS control.source_watermarks (
    -- Stable source identifier, such as "openfoodfacts".
    -- PRIMARY KEY guarantees there can only be one current
    -- watermark record for each source.
    source_name VARCHAR(100) PRIMARY KEY,
    -- The upper boundary of the most recently SUCCESSFUL
    -- extraction window.
    --
    -- We use TIMESTAMPTZ so the stored value represents an
    -- unambiguous point in time.
    watermark_ts TIMESTAMPTZ NOT NULL,
    -- When this control record itself was last changed.
    -- Useful operational metadata for troubleshooting.
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- The logical run that advanced this watermark.
    --
    -- This gives us traceability between database state and
    -- the raw S3/MinIO run that produced the committed data.
    run_id VARCHAR(64) NOT NULL
);