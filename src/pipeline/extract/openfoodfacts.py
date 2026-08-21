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
    retry=retry_if_exception_type(
        (
            httpx.TransportError,
            RetryableHTTPError,
        )
    ),
    wait=wait_exponential_jitter(
        initial=2,
        max=30,
        jitter=2,
    ),
    stop=stop_after_attempt(5),
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
    max_pages: int = 2,
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

        for page_number in range(
            1,
            max_pages + 1,
        ):

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