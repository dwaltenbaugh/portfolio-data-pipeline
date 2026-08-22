from datetime import date

import pytest

from pipeline.dq.openfoodfacts import (
    DataQualityError,
    validate_openfoodfacts_mart_batch,
)


class FakeCursor:
    """
    Minimal fake psycopg cursor.

    The mart DQ function only needs:
    - execute()
    - fetchone()
    - context-manager behavior
    """

    def __init__(self, result):
        self.result = result
        self.executed_query = None
        self.executed_params = None

    def execute(
        self,
        query,
        params,
    ) -> None:
        self.executed_query = query
        self.executed_params = params

    def fetchone(self):
        return self.result

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ):
        return False


class FakeConnection:
    """
    Minimal fake psycopg connection.

    cursor() returns our predefined FakeCursor.
    """

    def __init__(self, result):
        self.cursor_instance = FakeCursor(
            result=result,
        )

    def cursor(self):
        return self.cursor_instance


def test_validate_openfoodfacts_mart_batch_passes() -> None:
    """
    Mart DQ should pass when:

    - no staging rows are missing from dim_product
    - no staging update events are missing from the fact table
    """

    connection = FakeConnection(
        result=(
            0,  # missing_dimension_count
            0,  # missing_fact_count
        )
    )

    batch_date = date(
        2026,
        8,
        22,
    )

    validate_openfoodfacts_mart_batch(
        connection=connection,
        batch_date=batch_date,
    )

    # Verify that the function queried the requested batch.
    assert (
        connection
        .cursor_instance
        .executed_params
    ) == (
        batch_date,
    )


def test_validate_openfoodfacts_mart_batch_rejects_missing_dimension() -> None:
    """
    Mart DQ should fail when staging contains a product
    with no corresponding dim_product row.
    """

    connection = FakeConnection(
        result=(
            1,  # one missing dimension row
            0,
        )
    )

    with pytest.raises(
        DataQualityError,
        match="Mart dimension reconciliation failed",
    ):
        validate_openfoodfacts_mart_batch(
            connection=connection,
            batch_date=date(
                2026,
                8,
                22,
            ),
        )


def test_validate_openfoodfacts_mart_batch_rejects_missing_fact() -> None:
    """
    Mart DQ should fail when staging contains an update event
    with no corresponding fact_product_update row.
    """

    connection = FakeConnection(
        result=(
            0,
            1,  # one missing fact event
        )
    )

    with pytest.raises(
        DataQualityError,
        match="Mart fact reconciliation failed",
    ):
        validate_openfoodfacts_mart_batch(
            connection=connection,
            batch_date=date(
                2026,
                8,
                22,
            ),
        )