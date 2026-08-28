from datetime import UTC, datetime

import pytest

from pipeline.extract.openfoodfacts import (
    OpenFoodFactsConfig,
    _page_timestamp_bounds,
    _product_is_in_window,
    iter_product_pages,
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


def test_page_timestamp_bounds_returns_newest_and_oldest():
    """
    The helper should identify the chronological bounds of one
    source page regardless of the order of records in the list.
    """

    products = [
        {
            "code": "a",
            "last_modified_t": datetime(
                2026,
                8,
                1,
                tzinfo=UTC,
            ).timestamp(),
        },
        {
            "code": "b",
            "last_modified_t": datetime(
                2026,
                8,
                5,
                tzinfo=UTC,
            ).timestamp(),
        },
        {
            "code": "c",
            "last_modified_t": datetime(
                2026,
                7,
                28,
                tzinfo=UTC,
            ).timestamp(),
        },
    ]

    newest, oldest = _page_timestamp_bounds(
        products
    )

    assert newest == datetime(
        2026,
        8,
        5,
        tzinfo=UTC,
    )

    assert oldest == datetime(
        2026,
        7,
        28,
        tzinfo=UTC,
    )


def test_page_timestamp_bounds_returns_none_when_no_valid_timestamps():
    """
    If a page has no usable last_modified_t values, the extractor
    cannot infer chronological page boundaries.
    """

    products = [
        {"code": "a"},
        {
            "code": "b",
            "last_modified_t": None,
        },
    ]

    newest, oldest = _page_timestamp_bounds(
        products
    )

    assert newest is None
    assert oldest is None


def test_iter_product_pages_rejects_invalid_max_pages():
    config = OpenFoodFactsConfig(
        user_agent="portfolio-data-pipeline-test"
    )

    extract_start = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    extract_end = datetime(
        2026,
        8,
        21,
        tzinfo=UTC,
    )

    iterator = iter_product_pages(
        config=config,
        extract_start=extract_start,
        extract_end=extract_end,
        max_pages=0,
    )

    with pytest.raises(
        ValueError,
        match="max_pages must be at least 1",
    ):
        next(iterator)