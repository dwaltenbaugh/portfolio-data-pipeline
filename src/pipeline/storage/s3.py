import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


class S3UploadError(Exception):
    """Raised when an object cannot be uplaoded to S3-compatible storage."""

class ObjectAlreadyExistsError(Exception):
    """
    Raised when we intentionally try to create an object only if it
    does not already exist, but that object is already present.
    """


@dataclass(frozen=True)
class S3StorageConfig:
    bucket: str
    endpoint_url: str | None = None
    region_name: str = "us-east-1"
    access_key: str | None = None
    secret_key: str | None = None


def create_s3_client(
        config: S3StorageConfig,
) -> Any:
    """"Create an S3 client using the normal boto3 credential chain."""

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region_name,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        config=Config(
            retries={
                "mode": "standard",
                "max_attempts": 5,
            },
            s3={"addressing_style": "path",
            },
        ),
    )

def object_exists(
    client: Any,
    config: S3StorageConfig,
    object_key: str,
) -> bool:
    """
    Check whether a specific object already exists in our bucket.

    We use head_object instead of downloading the object because we
    only need to know whether the key exists. We do not need the
    object's actual contents.
    """
    try:
        # Ask S3/MinIO for the object's metadata.
        #
        # If the object exists, head_object succeeds.
        # We don't actually download the Parquet or JSON data.
        client.head_object(
            Bucket=config.bucket,
            Key=object_key
        )

        # If no exception was raised, the object exists.
        return True

    except ClientError as exc:
        # boto3 ClientError contains the HTTP/S3 error information
        # returned by the server. Pull out both the HTTP status code
        # and S3-specific error code so we can recognize "not found".
        status_code = (
            exc.response
            .get("ResponseMetadata", {})
            .get("HTTPStatusCode")
        )

        error_code = (
            exc.response
            .get("Error", {})
            .get("Code")
        )

         # A missing object may be represented slightly differently
        # depending on the S3-compatible implementation, so handle
        # the common "not found" responses.
        if (
            status_code == 404
            or error_code
            in {
                "404",
                "NoSuchKey",
                "NotFound",
            }
        ):
            return False

        # If this wasn't a normal "object doesn't exist" response,
        # DON'T pretend the object is missing.
        #
        # For example, a 403 could mean we lack permission to inspect
        # the object. That should be treated as an actual error.
        raise


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


def put_json_object(
    client: Any,
    config: S3StorageConfig,
    object_key: str,
    payload: dict[str, Any],
    *,
    if_absent: bool = False,
) -> str:
    """
    Serialize a Python dictionary to JSON and store it as an
    S3-compatible object.

    When if_absent=True, the write is conditional: S3 should create
    the object only if an object with this key does NOT already exist.
    """

    # Convert the Python dictionary into formatted JSON text.
    #
    # Example:
    #
    # {"row_count": 100}
    #
    # becomes JSON text similar to:
    #
    # {
    #   "row_count": 100
    # }
    json_text = json.dumps(
        payload,
        indent=2,
        sort_keys=True,
    )

    # S3 stores bytes, so encode our JSON string as UTF-8 bytes.
    body = json_text.encode("utf-8")

    # Build the normal arguments required by boto3's put_object().
    #
    # ContentType isn't required to store the object, but identifying
    # it as application/json correctly describes what we've written.
    put_kwargs: dict[str, Any] = {
        "Bucket": config.bucket,
        "Key": object_key,
        "Body": body,
        "ContentType": "application/json",
    }

    # For ordinary objects such as manifest.json, we'll allow the
    # normal PUT behavior.
    #
    # For our final _SUCCESS.json commit marker, we'll eventually call
    # this with if_absent=True.
    if if_absent:
        # IfNoneMatch="*" means:
        #
        # "Only perform this write if there is currently NO object
        # with this key."
        #
        # This protects our logical run's success marker from being
        # silently overwritten by another execution attempt.
        put_kwargs["IfNoneMatch"] = "*"

    try:
        # **put_kwargs expands the dictionary into keyword arguments.
        #
        # In other words:
        #
        # client.put_object(**put_kwargs)
        #
        # behaves like:
        #
        # client.put_object(
        #     Bucket=...,
        #     Key=...,
        #     Body=...,
        #     ContentType=...,
        # )
        client.put_object(
            **put_kwargs,
        )

    except ClientError as exc:
        # Inspect the HTTP status returned by S3/MinIO.
        status_code = (
            exc.response
            .get("ResponseMetadata", {})
            .get("HTTPStatusCode")
        )

        # When using IfNoneMatch="*", an existing object causes the
        # conditional write to fail with HTTP 412.
        #
        # That's an expected business condition for our pipeline, so
        # translate it into our application's custom exception.
        if if_absent and status_code == 412:
            raise ObjectAlreadyExistsError(
                "Object already exists: "
                f"s3://{config.bucket}/{object_key}"
            ) from exc

        # Any other S3 response represents a genuine write failure.
        raise S3UploadError(
            "Failed to write "
            f"s3://{config.bucket}/{object_key}"
        ) from exc

    except BotoCoreError as exc:
        # BotoCoreError represents lower-level boto problems such as
        # client/configuration/transport failures rather than a normal
        # error response returned by S3.
        raise S3UploadError(
            "Failed to write "
            f"s3://{config.bucket}/{object_key}"
        ) from exc

    # Give the caller a convenient URI identifying the object that
    # was successfully written.
    return (
        f"s3://{config.bucket}/"
        f"{object_key}"
    )

def get_json_object(
    client: Any,
    config: S3StorageConfig,
    object_key: str,
) -> dict[str, Any]:
    """
    Download a JSON object from S3-compatible storage and return
    its contents as a Python dictionary.

    We'll primarily use this to read _SUCCESS.json when reconciling
    raw-storage state with PostgreSQL watermark state.
    """

    try:
        # get_object returns metadata plus a streaming response body.
        response = client.get_object(
            Bucket=config.bucket,
            Key=object_key,
        )

        # Body is a StreamingBody rather than a normal string.
        # read() retrieves its bytes.
        body_bytes = response["Body"].read()

        # Decode UTF-8 bytes into text.
        body_text = body_bytes.decode("utf-8")

        # Convert the JSON text back into Python objects.
        payload = json.loads(body_text)

    except (BotoCoreError, ClientError) as exc:
        raise S3UploadError(
            "Failed to read "
            f"s3://{config.bucket}/{object_key}"
        ) from exc

    # We expect our control objects to contain JSON dictionaries,
    # not arrays, strings, numbers, etc.
    if not isinstance(payload, dict):
        raise S3UploadError(
            "Expected JSON object at "
            f"s3://{config.bucket}/{object_key}"
        )

    return payload

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


def get_object_bytes(
    client: Any,
    config: S3StorageConfig,
    object_key: str,
) -> bytes:
    """
    Download one object from S3-compatible storage and return
    its raw bytes.

    This is useful for binary formats such as Parquet.
    """

    try:
        response = client.get_object(
            Bucket=config.bucket,
            Key=object_key,
        )

        return cast(bytes, response["Body"].read())

    except (BotoCoreError, ClientError) as exc:
        raise S3UploadError(
            "Failed to read "
            f"s3://{config.bucket}/{object_key}"
        ) from exc
    