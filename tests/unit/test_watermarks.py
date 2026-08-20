from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from pipeline.state.watermarks import (
    WatermarkAdvanceError,
    WatermarkRepository,
)


def create_mock_repository():
    """
    Create a WatermarkRepository backed by mock database objects.

    This lets us test our repository's Python behavior without making
    a real PostgreSQL connection.
    """

    # Pretend this is a real psycopg Connection.
    connection = MagicMock()

    # Pretend this is a real psycopg Cursor.
    cursor = MagicMock()

    # Our production code uses:
    #
    #   with connection.cursor() as cursor:
    #
    # MagicMock therefore needs to behave like a context manager.
    connection.cursor.return_value.__enter__.return_value = cursor

    repository = WatermarkRepository(
        connection=connection,
    )

    return repository, cursor


def test_get_watermark_returns_existing_timestamp() -> None:
    repository, cursor = create_mock_repository()

    expected_watermark = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    # Simulate PostgreSQL returning one row containing one column.
    cursor.fetchone.return_value = (
        expected_watermark,
    )

    result = repository.get_watermark(
        "openfoodfacts"
    )

    assert result == expected_watermark


def test_get_watermark_returns_none_for_new_source() -> None:
    repository, cursor = create_mock_repository()

    # fetchone() returns None when SELECT found no matching row.
    cursor.fetchone.return_value = None

    result = repository.get_watermark(
        "openfoodfacts"
    )

    assert result is None


def test_advance_watermark_succeeds_when_row_changes() -> None:
    repository, cursor = create_mock_repository()

    # Simulate PostgreSQL successfully inserting/updating one row.
    cursor.rowcount = 1

    repository.advance_watermark(
        source_name="openfoodfacts",
        new_watermark=datetime(
            2026,
            8,
            20,
            tzinfo=UTC,
        ),
        run_id="abc123",
    )

    # Verify that SQL was actually sent to the mocked cursor.
    cursor.execute.assert_called_once()


    def test_advance_watermark_fails_when_row_does_not_change() -> None:
        repository, cursor = create_mock_repository()

        # PostgreSQL's WHERE condition prevented the upsert because the
        # new timestamp was not greater than the stored watermark.
        cursor.rowcount = 0

        with pytest.raises(
            WatermarkAdvanceError,
            match="not later than the existing watermark",
        ):
            repository.advance_watermark(
                source_name="openfoodfacts",
                new_watermark=datetime(
                    2026,
                    8,
                    19,
                    tzinfo=UTC,
                ),
                run_id="abc123",
            )