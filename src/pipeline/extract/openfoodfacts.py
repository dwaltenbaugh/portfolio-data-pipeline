import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)


logger = logging.getLogger(__name__)


BASE_URL = "https://world.openfoodfacts.org/api/v2/search"


class RetryableHTTPError(Exception):
    """Raised for HTTP errors that are safe to retry."""

@dataclass(frozen=True)
class OpenFoodFactsConfig:
    user_agent: str
    page_size: int = 50
    timeout_seconds: float = 30.0
    seconds_between_requests: float = 6.5


def _check_response(response: httpx.Response) -> None:
    """Classify HTTP responses as retrytable or non-retryable."""

    if response.status_code == 429:
        raise RetryableHTTPError(
            "Open Food Facts rate limit exceeded."
        )

    if response.status_code in {500, 502, 503, 504}:
        raise RetryableHTTPError(
            f"Transient server error: {response.status_code}"
        )

    response.raise_for_status()

@retry(
    retry=retry_if_exception_type(
        (httpx.TransportError, RetryableHTTPError)
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
    """Fetch one page from the Open Food Facts search API."""

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
        "page": page,
        "page_size": page_size,
    }

    logger.info("Requesting Open Food Facts page %s", page)

    response = client.get(
        BASE_URL,
        params=params,
    )

    _check_response(response)

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("Expected API response to be a JSON object.")

    return payload


def iter_product_pages(
    config: OpenFoodFactsConfig,
    max_pages: int = 2,
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    """Yield Open Food Facts products one page at a time."""

    timeout = httpx.Timeout(config.timeout_seconds)

    headers = {
        "User-Agent": config.user_agent,
    }

    with httpx.Client(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    ) as client:

        total_seen = 0

        for page_number in range(1, max_pages + 1):

            payload = fetch_page(
                client=client,
                page=page_number,
                page_size=config.page_size,
            )

            products = payload.get("products", [])

            if not isinstance(products, list):
                raise ValueError(
                    "Expected 'products' to contain a list."
                )

            if not products:
                logger.info("No products returned. Pagination complete.")
                break

            total_seen += len(products)

            logger.info(
                "Page %s returned %s products. Total seen: %s",
                page_number,
                len(products),
                total_seen,
            )

            yield page_number, products

            total_available = payload.get("count")

            if (
                isinstance(total_available, int)
                and total_seen >= total_available
            ):
                logger.info("All available products retrieved.")
                break

            if page_number < max_pages:
                time.sleep(config.seconds_between_requests)