from datetime import UTC, datetime, timedelta

import pytest

from pipeline.state.extraction_window import (
    ExtractionWindowError,
    build_extraction_window,
)


def test_new_source_starts_at_initial_watermark() -> None:
    """
    A source with no stored watermark should begin at its
    configured initial extraction timestamp.
    """

    initial = datetime(
        2026,
        8,
        1,
        tzinfo=UTC,
    )

    batch_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    window = build_extraction_window(
        source_name="openfoodfacts",
        current_watermark=None,
        initial_watermark=initial,
        batch_end=batch_end,
    )

    assert window.previous_watermark is None
    assert window.incremental_start == initial
    assert window.extract_start == initial
    assert window.extract_end == batch_end
    assert window.next_watermark == batch_end


def test_existing_source_starts_at_current_watermark() -> None:
    """
    Without a lookback, an existing source should begin reading
    exactly where the previous successful run ended.
    """

    initial = datetime(
        2026,
        8,
        1,
        tzinfo=UTC,
    )

    current = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    batch_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    window = build_extraction_window(
        source_name="openfoodfacts",
        current_watermark=current,
        initial_watermark=initial,
        batch_end=batch_end,
    )

    assert window.previous_watermark == current
    assert window.incremental_start == current
    assert window.extract_start == current
    assert window.extract_end == batch_end
    assert window.next_watermark == batch_end


def test_lookback_moves_extract_start_backward() -> None:
    """
    A lookback should move the physical extraction start backward
    without moving the logical watermark backward.
    """

    initial = datetime(
        2026,
        8,
        1,
        tzinfo=UTC,
    )

    current = datetime(
        2026,
        8,
        19,
        tzinfo=UTC,
    )

    batch_end = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    window = build_extraction_window(
        source_name="openfoodfacts",
        current_watermark=current,
        initial_watermark=initial,
        batch_end=batch_end,
        lookback=timedelta(hours=6),
    )

    # New incremental data still logically begins Aug 19.
    assert (
        window.incremental_start
        == current
    )

    # But we physically re-read the final six hours before it.
    assert window.extract_start == datetime(
        2026,
        8,
        18,
        18,
        tzinfo=UTC,
    )

    # Successful completion would still move the high-water
    # mark FORWARD to Aug 20, never backward to Aug 18.
    assert window.next_watermark == batch_end


def test_lookback_does_not_cross_initial_watermark() -> None:
    """
    The lookback may not cause us to extract data from before
    the source's configured initial boundary.
    """

    initial = datetime(
        2026,
        8,
        1,
        tzinfo=UTC,
    )

    current = datetime(
        2026,
        8,
        1,
        2,
        tzinfo=UTC,
    )

    batch_end = datetime(
        2026,
        8,
        2,
        tzinfo=UTC,
    )

    # A six-hour lookback would mathematically produce:
    #
    # July 31 20:00
    #
    # but our configured history begins Aug 1, so we clamp
    # the extraction start to Aug 1.
    window = build_extraction_window(
        source_name="openfoodfacts",
        current_watermark=current,
        initial_watermark=initial,
        batch_end=batch_end,
        lookback=timedelta(hours=6),
    )

    assert window.extract_start == initial


def test_batch_end_must_move_past_current_watermark() -> None:
    """
    A logical run must advance beyond the existing high-water mark.
    """

    current = datetime(
        2026,
        8,
        20,
        tzinfo=UTC,
    )

    with pytest.raises(
        ExtractionWindowError,
        match="batch_end must be later",
    ):
        build_extraction_window(
            source_name="openfoodfacts",
            current_watermark=current,
            initial_watermark=datetime(
                2026,
                8,
                1,
                tzinfo=UTC,
            ),

            # Same timestamp as the current watermark:
            # there is no new logical interval.
            batch_end=current,
        )


def test_naive_datetime_is_rejected() -> None:
    """
    Pipeline timestamps must identify an unambiguous point in time.
    """

    # No tzinfo means this datetime is "naive".
    naive_initial = datetime(
        2026,
        8,
        1,
    )

    with pytest.raises(
        ExtractionWindowError,
        match="timezone-aware",
    ):
        build_extraction_window(
            source_name="openfoodfacts",
            current_watermark=None,
            initial_watermark=naive_initial,
            batch_end=datetime(
                2026,
                8,
                20,
                tzinfo=UTC,
            ),
        )