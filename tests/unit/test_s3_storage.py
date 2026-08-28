import os
from datetime import date
from pathlib import Path

from pipeline.storage.s3 import (
    S3StorageConfig,
    build_raw_object_key,
    upload_file,
)


class FakeS3Client:
    def __init__(self) -> None:
        self.uploaded: list[
            tuple[str, str, str]
        ] = []

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
    ) -> None:
        self.uploaded.append(
            (filename, bucket, key)
        )

    def head_object(
        self,
        Bucket: str,
        Key: str,
    ) -> dict:
        return {
            "Bucket": Bucket,
            "Key": Key,
        }


def test_build_raw_object_key() -> None:
    result = build_raw_object_key(
        source="openfoodfacts",
        extract_date=date(2026, 8, 18),
        part_number=3,
    )

    assert result == (
        "openfoodfacts/"
        "extract_date=2026-08-18/"
        "part-00003.parquet"
    )


def test_upload_file(
        tmp_path: Path,
) -> None:
    local_file = tmp_path / "part-00001.parquet"

    local_file.write_bytes(
        b"fake parquet contents"
    )

    client = FakeS3Client()

    config = S3StorageConfig(
        bucket="portfolio-data-raw",
        endpoint_url="http://localhost:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
    )

    uri = upload_file(
        client=client,
        local_path=local_file,
        config=config,
        object_key=(
            "openfoodfacts/"
            "extract_date=2026-08-18/"
            "part-00001.parquet"
        ),
    )

    assert uri == (
        "s3://portfolio-data-raw/"
        "openfoodfacts/"
        "extract_date=2026-08-18/"
        "part-00001.parquet"
    )

    assert client.uploaded == [
        (
            str(local_file),
            "portfolio-data-raw",
            "openfoodfacts/"
            "extract_date=2026-08-18/"
            "part-00001.parquet",
        )
    ]