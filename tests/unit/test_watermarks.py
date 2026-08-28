from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from pipeline.state.watermarks import (
    WatermarkAdvanceError,
    WatermarkRepository,
    reconcile_watermark,
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


def test_reconcile_initializes_missing_watermark() -> None:
    """
    If raw storage has committed successfully but PostgreSQL has no
    state yet, reconciliation should create the watermark.
    """

    repository = MagicMock()

    committed_watermark = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    # Simulate a brand-new source.
    repository.get_watermark.return_value = None

    changed = reconcile_watermark(
        repository=repository,
        source_name="openfoodfacts",
        committed_watermark=committed_watermark,
        run_id="run-001",
    )

    assert changed is True

    repository.advance_watermark.assert_called_once_with(
        source_name="openfoodfacts",
        new_watermark=committed_watermark,
        run_id="run-001",
    )

def test_reconcile_advances_behind_watermark() -> None:
    """
    If PostgreSQL is behind the committed raw watermark, advance it.
    """

    repository = MagicMock()

    repository.get_watermark.return_value = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    committed_watermark = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    changed = reconcile_watermark(
        repository=repository,
        source_name="openfoodfacts",
        committed_watermark=committed_watermark,
        run_id="run-002",
    )

    assert changed is True

    repository.advance_watermark.assert_called_once_with(
        source_name="openfoodfacts",
        new_watermark=committed_watermark,
        run_id="run-002",
    )

def test_reconcile_does_nothing_when_watermark_matches() -> None:
    """
    Reconciliation should be idempotent.

    If PostgreSQL already contains the committed watermark, there
    is nothing left to do.
    """

    repository = MagicMock()

    committed_watermark = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    repository.get_watermark.return_value = (
        committed_watermark
    )

    changed = reconcile_watermark(
        repository=repository,
        source_name="openfoodfacts",
        committed_watermark=committed_watermark,
        run_id="run-002",
    )

    assert changed is False

    repository.advance_watermark.assert_not_called()

def test_reconcile_never_moves_watermark_backward() -> None:
    """
    If PostgreSQL is already ahead of an older committed raw run,
    reconciliation must not move state backward.
    """

    repository = MagicMock()

    repository.get_watermark.return_value = datetime(
        2026,
        8,
        21,
        tzinfo=UTC,
    )

    older_committed_watermark = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    changed = reconcile_watermark(
        repository=repository,
        source_name="openfoodfacts",
        committed_watermark=older_committed_watermark,
        run_id="old-run",
    )

    assert changed is False

    repository.advance_watermark.assert_not_called()