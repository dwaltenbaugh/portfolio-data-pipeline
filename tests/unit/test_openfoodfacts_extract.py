from datetime import UTC, datetime

from pipeline.extract.openfoodfacts import (
    _product_is_in_window,
)


def test_product_inside_extraction_window():
    """
    A product modified after extract_start and before extract_end
    belongs to this run.
    """

    extract_start = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    extract_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    modified_at = datetime(
        2026,
        8,
        19,
        12,
        0,
        tzinfo=UTC,
    )

    product = {
        "code": "123",
        "last_modified_t": (
            modified_at.timestamp()
        ),
    }

    assert _product_is_in_window(
        product,
        extract_start=extract_start,
        extract_end=extract_end,
    )


def test_product_at_extract_start_is_included():
    """
    The extraction window has an inclusive lower boundary.
    """

    extract_start = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    extract_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    product = {
        "code": "123",
        "last_modified_t": (
            extract_start.timestamp()
        ),
    }

    assert _product_is_in_window(
        product,
        extract_start=extract_start,
        extract_end=extract_end,
    )


def test_product_at_extract_end_is_excluded():
    """
    The extraction window has an exclusive upper boundary.

    This prevents adjacent logical runs from both claiming the
    exact extract_end timestamp.
    """

    extract_start = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    extract_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    product = {
        "code": "123",
        "last_modified_t": (
            extract_end.timestamp()
        ),
    }

    assert not _product_is_in_window(
        product,
        extract_start=extract_start,
        extract_end=extract_end,
    )

def test_product_before_extract_start_is_excluded():
    extract_start = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    extract_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    modified_at = datetime(
        2026,
        8,
        18,
        23,
        59,
        tzinfo=UTC,
    )

    product = {
        "code": "123",
        "last_modified_t": (
            modified_at.timestamp()
        ),
    }

    assert not _product_is_in_window(
        product,
        extract_start=extract_start,
        extract_end=extract_end,
    )


def test_product_without_last_modified_timestamp_is_excluded():
    extract_start = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    extract_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    product = {
        "code": "123",
    }

    assert not _product_is_in_window(
        product,
        extract_start=extract_start,
        extract_end=extract_end,
    )