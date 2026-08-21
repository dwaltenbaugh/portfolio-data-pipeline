import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)


logger = logging.getLogger(__name__)


BASE_URL = (
    "https://world.openfoodfacts.org/api/v2/search"
)


class RetryableHTTPError(Exception):
    """Raised for HTTP errors that are safe to retry."""


@dataclass(frozen=True)
class OpenFoodFactsConfig:
    user_agent: str
    page_size: int = 50
    timeout_seconds: float = 30.0
    seconds_between_requests: float = 6.5


def _to_utc(
    value: datetime,
    field_name: str,
) -> datetime:
    """
    Require an aware datetime and normalize it to UTC.

    Extraction boundaries represent exact instants. Allowing a
    timezone-naive datetime could cause records to be assigned to
    the wrong incremental batch.
    """

    if value.utcoffset() is None:
        raise ValueError(
            f"{field_name} must be timezone-aware."
        )

    return value.astimezone(UTC)


def _product_is_in_window(
    product: dict[str, Any],
    *,
    extract_start: datetime,
    extract_end: datetime,
) -> bool:
    """
    Determine whether one Open Food Facts product belongs to this
    incremental extraction.

    Window semantics:

        extract_start <= last_modified_t < extract_end

    Open Food Facts provides last_modified_t as Unix epoch seconds.
    """

    last_modified_t = product.get(
        "last_modified_t"
    )

    # Incremental extraction requires a usable modification
    # timestamp. Without one, we cannot safely determine which
    # logical run owns the record.
    if not isinstance(
        last_modified_t,
        (int, float),
    ):
        logger.warning(
            (
                "Skipping product %s because "
                "last_modified_t is missing or invalid."
            ),
            product.get("code"),
        )

        return False

    modified_at = datetime.fromtimestamp(
        last_modified_t,
        tz=UTC,
    )

    return (
        extract_start
        <= modified_at
        < extract_end
    )


def _page_timestamp_bounds(
    products: list[dict[str, Any]],
) -> tuple[datetime | None, datetime | None]:
    """
    Return the newest and oldest valid modification timestamps
    found on one Open Food Facts page.

    The result is:

        (newest_timestamp, oldest_timestamp)

    None is returned for both values if the page contains no usable
    last_modified_t values.

    This helper will eventually let our pagination loop reason about
    whether additional pages could still contain records belonging
    to the current extraction window.
    """

    timestamps: list[datetime] = []

    for product in products:
        last_modified_t = product.get(
            "last_modified_t"
        )

        # Some source records may not have a usable modification
        # timestamp.
        #
        # Ignore those records for the purpose of determining the
        # page's chronological boundaries.
        if not isinstance(
            last_modified_t,
            (int, float),
        ):
            continue

        modified_at = datetime.fromtimestamp(
            last_modified_t,
            tz=UTC,
        )

        timestamps.append(
            modified_at
        )

    # If there were no usable timestamps, we cannot determine this
    # page's chronological position.
    if not timestamps:
        return None, None

    # max() gives us the most recent timestamp.
    newest_timestamp = max(
        timestamps
    )

    # min() gives us the oldest timestamp.
    oldest_timestamp = min(
        timestamps
    )

    return (
        newest_timestamp,
        oldest_timestamp,
    )


def _check_response(
    response: httpx.Response,
) -> None:
    """Classify HTTP responses as retryable or non-retryable."""

    if response.status_code == 429:
        raise RetryableHTTPError(
            "Open Food Facts rate limit exceeded."
        )

    if response.status_code in {
        500,
        502,
        503,
        504,
    }:
        raise RetryableHTTPError(
            (
                "Transient server error: "
                f"{response.status_code}"
            )
        )

    response.raise_for_status()


@retry(
    # Retry only failures we have explicitly classified as
    # transient. Programming errors, bad JSON, schema failures,
    # 400 responses, etc. should still fail immediately.
    retry=retry_if_exception_type(
        (
            httpx.TransportError,
            RetryableHTTPError,
        )
    ),

    # Wait progressively longer between failed attempts.
    #
    # This is especially important for Open Food Facts because
    # HTTP 503 may represent infrastructure-wide throttling rather
    # than a brief network hiccup.
    #
    # Roughly:
    #     attempt 1 -> ~5 sec
    #     attempt 2 -> ~10 sec
    #     attempt 3 -> ~20 sec
    #     attempt 4 -> ~40 sec
    #     attempt 5+ -> capped near 120 sec
    #
    # Jitter keeps multiple clients from retrying at exactly the
    # same moment.
    wait=wait_exponential_jitter(
        initial=5,
        max=120,
        jitter=5,
    ),

    # Give a degraded public API more time to recover before
    # abandoning the extraction.
    stop=stop_after_attempt(8),

    # If every attempt fails, propagate the final exception so the
    # pipeline run fails rather than silently continuing.
    reraise=True,
)


def fetch_page(
    client: httpx.Client,
    page: int,
    page_size: int,
) -> dict[str, Any]:
    """
    Fetch one page from the Open Food Facts search API.

    Timestamp-window filtering happens after the response is
    returned because the v2 search endpoint does not expose our
    desired extract_start/extract_end semantics directly.
    """

    params = {
        "categories_tags_en": "Orange Juice",

        "fields": (
            "code,"
            "product_name,"
            "brands,"
            "nutrition_grades,"
            "categories_tags_en,"
            "last_modified_t"
        ),

        # Incremental extraction is based on modification time,
        # so ask the API to organize search results around that
        # same source field.
        "sort_by": "last_modified_t",

        "page": page,
        "page_size": page_size,
    }

    logger.info(
        "Requesting Open Food Facts page %s",
        page,
    )

    response = client.get(
        BASE_URL,
        params=params,
    )

    _check_response(response)

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError(
            "Expected API response to be a JSON object."
        )

    return payload


def iter_product_pages(
    config: OpenFoodFactsConfig,
    extract_start: datetime,
    extract_end: datetime,
    max_pages: int | None = None,
) -> Iterator[
    tuple[int, list[dict[str, Any]]]
]:
    """
    Yield Open Food Facts products one API page at a time.

    Only records satisfying:

        extract_start <= last_modified_t < extract_end

    are yielded to the downstream raw-layer writer.
    """

    # Protect this function even if it is eventually called
    # somewhere other than our ExtractionWindow workflow.
    extract_start = _to_utc(
        extract_start,
        "extract_start",
    )

    extract_end = _to_utc(
        extract_end,
        "extract_end",
    )

    if extract_end <= extract_start:
        raise ValueError(
            (
                "extract_end must be later "
                "than extract_start."
            )
        )

    # If a caller supplies a safety cap, it must allow at least
    # one source page to be requested.
    if (
        max_pages is not None
        and max_pages < 1
    ):
        raise ValueError(
            "max_pages must be at least 1 when provided."
        )

    logger.info(
        (
            "Open Food Facts extraction window: "
            "[%s, %s)"
        ),
        extract_start,
        extract_end,
    )

    timeout = httpx.Timeout(
        config.timeout_seconds
    )

    headers = {
        "User-Agent": config.user_agent,
    }

    with httpx.Client(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    ) as client:

        # Number of source records inspected across API pages.
        #
        # This is intentionally NOT the number written to raw
        # storage because some records will be outside the window.
        total_seen = 0

        page_number = 1

        while True:

            payload = fetch_page(
                client=client,
                page=page_number,
                page_size=config.page_size,
            )

            products = payload.get(
                "products",
                [],
            )

            if not isinstance(products, list):
                raise ValueError(
                    (
                        "Expected 'products' "
                        "to contain a list."
                    )
                )

            if not products:
                logger.info(
                    (
                        "No products returned. "
                        "Pagination complete."
                    )
                )
                break

            total_seen += len(products)

            # Determine the chronological range represented by
            # this source page.
            #
            # This does NOT change filtering behavior yet.
            # For now, it is diagnostic information that will help
            # us verify how Open Food Facts orders paginated results
            # when sort_by=last_modified_t.
            (
            page_newest_timestamp,
            page_oldest_timestamp,
            ) = _page_timestamp_bounds(
                products
            )

            logger.info(
                (
                    "Page %s timestamp range: "
                    "newest=%s, oldest=%s"
                ),
                page_number,
                page_newest_timestamp,
                page_oldest_timestamp,
            )

            # If even the newest record on this page is older than the
            # extraction window, then the entire page is too old.
            #
            # Because Open Food Facts is returning pages in descending
            # last_modified_t order, subsequent pages will be older still.
            if (
                page_newest_timestamp is not None
                and page_newest_timestamp < extract_start
            ):
                logger.info(
                    (
                        "Page %s is entirely older than "
                        "extract_start=%s. "
                        "Pagination complete."
                    ),
                    page_number,
                    extract_start,
                )

                break

            # Apply the pipeline's actual incremental extraction
            # contract to every record returned by the API.
            products_in_window = [
                product
                for product in products
                if _product_is_in_window(
                    product,
                    extract_start=extract_start,
                    extract_end=extract_end,
                )
            ]

            logger.info(
                (
                    "Page %s returned %s products; "
                    "%s are inside the window. "
                    "Total source products seen: %s"
                ),
                page_number,
                len(products),
                len(products_in_window),
                total_seen,
            )

            # Do not create empty raw Parquet parts.
            if products_in_window:
                yield (
                    page_number,
                    products_in_window,
                )

            total_available = payload.get(
                "count"
            )

            if (
                isinstance(
                    total_available,
                    int,
                )
                and total_seen
                >= total_available
            ):
                logger.info(
                    (
                        "All available products "
                        "retrieved."
                    )
                )
                break

            if page_number < max_pages:
                time.sleep(
                    config.seconds_between_requests
                )

            # --------------------------------------------------
            # OPTIONAL SAFETY CAP
            # --------------------------------------------------

            # max_pages is no longer how we decide that the extraction
            # window is complete.
            #
            # It is only an optional development / operational guard that
            # prevents an unexpectedly large number of requests.
            if (
                max_pages is not None
                and page_number >= max_pages
            ):
                logger.warning(
                    (
                        "Reached max_pages=%s before the source "
                        "pagination naturally completed."
                    ),
                    max_pages,
                )

                break

            # Respect the configured delay before requesting another page.
            time.sleep(
                config.seconds_between_requests
            )

            # Advance to the next API page only after all stopping conditions
            # for the current page have been evaluated.
            page_number += 1    