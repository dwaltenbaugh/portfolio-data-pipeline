from datetime import date

from psycopg import Connection


def record_dq_metric(
    *,
    connection: Connection,
    source_name: str,
    pipeline_layer: str,
    batch_date: date,
    metric_name: str,
    metric_value: int,
    passed: bool,
    expected_value: int | None = None,
) -> None:
    """
    Persist one logical data-quality metric.

    Re-running the same batch updates the existing metric instead
    of creating a duplicate row.
    """

    upsert_query = """
        INSERT INTO control.dq_metrics (
            source_name,
            pipeline_layer,
            batch_date,
            metric_name,
            metric_value,
            passed,
            expected_value
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )

        ON CONFLICT (
            source_name,
            pipeline_layer,
            batch_date,
            metric_name
        )
        DO UPDATE SET
            metric_value = EXCLUDED.metric_value,
            passed = EXCLUDED.passed,
            expected_value = EXCLUDED.expected_value,
            recorded_at = CURRENT_TIMESTAMP
    """

    with connection.cursor() as cursor:
        cursor.execute(
            upsert_query,
            (
                source_name,
                pipeline_layer,
                batch_date,
                metric_name,
                metric_value,
                passed,
                expected_value,
            ),
        )