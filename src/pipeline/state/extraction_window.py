from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


class ExtractionWindowError(Exception):
    """
    Raised when an incremental extraction window cannot be constructed.

    Examples:
      - naive datetimes without timezone information
      - a negative lookback
      - an extraction end that is not later than the current watermark
    """

@dataclass(frozen=True)
class ExtractionWindow:
    """
    Describe the time boundaries for one incremental source extraction.

    There are intentionally several related timestamps here because
    they represent different concepts.

    previous_watermark:
        The high-water mark currently stored in PostgreSQL.
        None means the source has never completed a run.

    incremental_start:
        The logical point immediately after previously committed work.
        For an existing source, this is the previous watermark.
        For a new source, this is the configured initial watermark.

    extract_start:
        Where we ACTUALLY begin reading source data.

        This may be earlier than incremental_start because of our
        late-arriving-data lookback.

    extract_end:
        The exclusive upper boundary of this extraction.

    next_watermark:
        The value we are allowed to persist ONLY AFTER the raw run
        successfully commits.

        For our current design, this is the same timestamp as
        extract_end.
    """

    source_name: str
    previous_watermark: datetime | None
    incremental_start: datetime
    extract_start: datetime
    extract_end: datetime
    next_watermark: datetime

def _to_utc(
    value: datetime,
    field_name: str,
) -> datetime:
    """
    Validate that a datetime is timezone-aware and normalize it to UTC.

    A timestamp like:

        2026-08-20 00:00:00

    is ambiguous because it doesn't tell us which timezone it belongs to.

    A timestamp like:

        2026-08-20 00:00:00+00:00

    represents an exact point in time.

    Pipeline watermarks should always represent exact points in time,
    so we reject naive datetimes.
    """

    # For a naive datetime, utcoffset() returns None.
    if value.utcoffset() is None:
        raise ExtractionWindowError(
            f"{field_name} must be timezone-aware."
        )

    # Convert any timezone-aware value to UTC so comparisons and
    # persisted state use one consistent timezone.
    return value.astimezone(UTC)


def build_extraction_window(
    source_name: str,
    current_watermark: datetime | None,
    initial_watermark: datetime,
    batch_end: datetime,
    lookback: timedelta = timedelta(0),
) -> ExtractionWindow:
    """
    Build the incremental extraction boundaries for one source run.

    The function performs three major jobs:

      1. Determine where new/unprocessed data logically begins.
      2. Apply an optional lookback for late-arriving data.
      3. Define the watermark that may be committed after success.

    This function does NOT read or update PostgreSQL.
    It only calculates the window.
    """

    # ----------------------------------------------------------
    # 1. VALIDATE AND NORMALIZE INPUT TIMESTAMPS
    # ----------------------------------------------------------

    # The initial watermark defines the earliest point from which
    # this pipeline is configured to extract data.
    initial_watermark = _to_utc(
        initial_watermark,
        "initial_watermark",
    )

    # batch_end represents the upper boundary of THIS logical run.
    batch_end = _to_utc(
        batch_end,
        "batch_end",
    )

    # PostgreSQL will normally return a timezone-aware TIMESTAMPTZ.
    # Normalize it to UTC if an existing watermark is present.
    if current_watermark is not None:
        current_watermark = _to_utc(
            current_watermark,
            "current_watermark",
        )

    # A lookback only makes sense as a zero-or-positive amount
    # of time. A negative lookback would actually move our
    # extraction start FORWARD and could cause skipped data.
    if lookback < timedelta(0):
        raise ExtractionWindowError(
            "lookback cannot be negative."
        )

    # ----------------------------------------------------------
    # 2. DETERMINE THE LOGICAL INCREMENTAL START
    # ----------------------------------------------------------

    if current_watermark is None:
        # This source has never successfully completed a run.
        #
        # Begin at the configured initial point.
        incremental_start = initial_watermark

    else:
        # This source has run before.
        #
        # Everything before current_watermark has already been
        # successfully committed, so new incremental work begins here.
        incremental_start = current_watermark

        # This would indicate inconsistent configuration/state.
        #
        # For example:
        #
        #   initial_watermark = Aug 20
        #   stored watermark  = Aug 19
        #
        # Rather than silently inventing behavior, fail explicitly.
        if current_watermark < initial_watermark:
            raise ExtractionWindowError(
                "current_watermark cannot be earlier "
                "than initial_watermark."
            )

    # ----------------------------------------------------------
    # 3. VERIFY THAT THIS RUN ACTUALLY MOVES FORWARD
    # ----------------------------------------------------------

    # We require the end of this batch to be later than the
    # logical starting point.
    #
    # For example:
    #
    # previous watermark = Aug 19
    # batch_end          = Aug 20
    #
    # is valid.
    #
    # But:
    #
    # previous watermark = Aug 20
    # batch_end          = Aug 20
    #
    # has no new interval to process.
    if batch_end <= incremental_start:
        raise ExtractionWindowError(
            "batch_end must be later than "
            "the incremental start."
        )

    # ----------------------------------------------------------
    # 4. APPLY THE LATE-ARRIVING-DATA LOOKBACK
    # ----------------------------------------------------------

    # Re-read some time immediately before the previous watermark.
    #
    # Example:
    #
    # incremental_start = Aug 19 00:00
    # lookback          = 6 hours
    #
    # candidate_start   = Aug 18 18:00
    candidate_start = (
        incremental_start - lookback
    )

    # Never read earlier than the configured initial watermark.
    #
    # This matters particularly for the first few runs.
    extract_start = max(
        initial_watermark,
        candidate_start,
    )

    # ----------------------------------------------------------
    # 5. RETURN THE COMPLETE WINDOW
    # ----------------------------------------------------------

    return ExtractionWindow(
        source_name=source_name,

        # What PostgreSQL currently knows about successful work.
        previous_watermark=current_watermark,

        # Where genuinely new data begins.
        incremental_start=incremental_start,

        # Where source reading actually begins after lookback.
        extract_start=extract_start,

        # Upper boundary of this run.
        extract_end=batch_end,

        # This becomes the new stored high-water mark only
        # after the raw run successfully commits.
        next_watermark=batch_end,
    )