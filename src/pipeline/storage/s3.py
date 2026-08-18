from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


class S3UploadError(Exception):
    """Raised when an object cannot be uplaoded to S3-compatible storage."""


@dataclass(frozen=True)
class S3StorageConfig:
    bucket: str
    endpoint_url: str | None = None
    region_name: str = "us-east-1"


def create_s3_client(
        config: S3StorageConfig,
) -> Any:
    """"Create an S3 client using the normal boto3 credential chain."""

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region_name,
        config=Config(
            retries={
                "mode": "standard",
                "max_attempts": 5,
            },
            s3={"addressing_style": "path",
            },
        ),
    )


def build_raw_object_key(
    source: str,
    extract_date: date,
    part_number: int,
) -> str:
    """Build the raw-layer object key for one extracted chunk."""

    return (
        f"{source}/"
        f"extract_date={extract_date.isoformat()}/"
        f"part-{part_number:05d}.parquet"
    )


def upload_file(
        client: Any,
        local_path: Path,
        config: S3StorageConfig,
        object_key: str,
) -> str:
    """Upload one local file and verify that the object exists."""

    try:
        client.upload_file(
            str(local_path),
            config.bucket,
            object_key
        )

        client.head_object(
            Bucket=config.bucket,
            Key=object_key,
        )

    except (BotoCoreError, ClientError) as exc:
        raise S3UploadError(
            f"Failed to upload s3://{config.bucket}/{object_key}"
        ) from exc

    return f"s3://{config.bucket}/{object_key}"