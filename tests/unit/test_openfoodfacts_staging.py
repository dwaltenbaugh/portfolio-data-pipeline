from datetime import date
import os

from pipeline.jobs import openfoodfacts_staging
from pipeline.storage.s3 import S3StorageConfig
import pytest


def test_get_committed_raw_object_keys(
    monkeypatch,
) -> None:
    """
    Verify that staging discovers raw Parquet files by following:

        _SUCCESS.json
            ↓
        manifest.json
            ↓
        objects[].object_key
    """

    # We do not need a real S3 client because get_json_object()
    # will be replaced by our fake function below.
    fake_s3_client = object()

    s3_config = S3StorageConfig(
        bucket="portfolio-data-raw",
        endpoint_url="http://localhost:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
    )

    batch_date = date(
        2026,
        8,
        21,
    )

    # Keep track of the S3 keys the function tries to read.
    requested_keys: list[str] = []

    manifest_key = (
        "openfoodfacts/"
        "extract_date=2026-08-21/"
        "run_id=test-run/"
        "attempt_id=test-attempt/"
        "manifest.json"
    )

    def fake_get_json_object(
        client,
        config,
        object_key,
    ):
        """
        Simulate the two S3 JSON reads performed by the staging job.
        """

        requested_keys.append(
            object_key
        )

        # First read:
        # staging should locate the logical run's _SUCCESS marker.
        if object_key.endswith(
            "/_SUCCESS.json"
        ):
            return {
                "manifest_key": manifest_key,
            }

        # Second read:
        # _SUCCESS points staging to the winning attempt's manifest.
        if object_key == manifest_key:
            return {
                "row_count": 6,
                "objects": [
                    {
                        "object_key": (
                            "openfoodfacts/"
                            "part-00001.parquet"
                        ),
                        "row_count": 4,
                        "size_bytes": 1000,
                    },
                    {
                        "object_key": (
                            "openfoodfacts/"
                            "part-00002.parquet"
                        ),
                        "row_count": 2,
                        "size_bytes": 500,
                    },
                ],
            }

        raise AssertionError(
            f"Unexpected object key: {object_key}"
        )

    # Replace the real S3 read with our fake.
    #
    # Importantly, patch the name where the staging module uses it.
    monkeypatch.setattr(
        openfoodfacts_staging,
        "get_json_object",
        fake_get_json_object,
    )

    (
    raw_object_keys,
    expected_row_count,
    ) = (
        openfoodfacts_staging
        .get_committed_raw_batch(
            s3_client=fake_s3_client,
            s3_config=s3_config,
            batch_date=batch_date,
        )
    )

    assert raw_object_keys == [
    "openfoodfacts/part-00001.parquet",
    "openfoodfacts/part-00002.parquet",
    ]

    assert expected_row_count == 6

    # It should have performed exactly two metadata reads:
    #
    # 1. canonical _SUCCESS marker
    # 2. manifest referenced by _SUCCESS
    assert len(requested_keys) == 2

    assert requested_keys[0].endswith(
        "/_SUCCESS.json"
    )

    assert requested_keys[1] == manifest_key


def test_get_committed_raw_batch_rejects_row_count_mismatch(
    monkeypatch,
) -> None:
    """
    Reject a manifest whose declared row count does not equal
    the sum of its object-level row counts.
    """

    fake_s3_client = object()

    s3_config = S3StorageConfig(
        bucket="portfolio-data-raw",
        endpoint_url="http://localhost:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
    )

    batch_date = date(
        2026,
        8,
        21,
    )

    manifest_key = "test/manifest.json"

    def fake_get_json_object(
        client,
        config,
        object_key,
    ):
        if object_key.endswith(
            "/_SUCCESS.json"
        ):
            return {
                "manifest_key": manifest_key,
            }

        if object_key == manifest_key:
            return {
                # Deliberately incorrect:
                # objects total 6, but manifest claims 7.
                "row_count": 7,
                "objects": [
                    {
                        "object_key": "part-1.parquet",
                        "row_count": 4,
                        "size_bytes": 1000,
                    },
                    {
                        "object_key": "part-2.parquet",
                        "row_count": 2,
                        "size_bytes": 500,
                    },
                ],
            }

        raise AssertionError(
            f"Unexpected object key: {object_key}"
        )

    monkeypatch.setattr(
        openfoodfacts_staging,
        "get_json_object",
        fake_get_json_object,
    )

    with pytest.raises(
        openfoodfacts_staging.DataQualityError,
        match="Raw manifest row-count mismatch",
    ):
        openfoodfacts_staging.get_committed_raw_batch(
            s3_client=fake_s3_client,
            s3_config=s3_config,
            batch_date=batch_date,
        )