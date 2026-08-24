CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.dq_metrics (
    dq_metric_key BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    -- Logical pipeline source.
    source_name TEXT NOT NULL,

    -- Pipeline layer where the metric was measured.
    -- Examples: raw, staging, mart.
    pipeline_layer TEXT NOT NULL,

    -- Logical batch associated with this measurement.
    batch_date DATE NOT NULL,

    -- Human-readable metric identifier.
    -- Examples:
    --   row_count
    --   null_code_count
    --   missing_dimension_count
    --   missing_fact_count
    metric_name TEXT NOT NULL,

    -- Numeric result of the DQ measurement.
    metric_value BIGINT NOT NULL,

    -- Whether this metric satisfied its quality rule.
    passed BOOLEAN NOT NULL,

    -- Optional threshold or expectation for context.
    expected_value BIGINT,

    -- When the metric was recorded.
    recorded_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP

    UNIQUE (
        source_name,
        pipeline_layer,
        batch_date,
        metric_name
    )
);