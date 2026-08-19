from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5


@dataclass(frozen=True)
class RawRunContext:
    source: str
    logical_date: date
    run_id: str
    attempt_id: str
    started_at: datetime


@dataclass(frozen=True)
class RawObjectRecord:
    object_key: str
    row_count: int
    size_bytes: int


def create_raw_run_context(
    source: str,
    logical_date: date,
) -> RawRunContext:
    """Create stable logical-run identity plus unique execution attempt."""

    run_id = uuid5(
        NAMESPACE_URL,
        (
            "portfolio-data-pipeline:"
            f"{source}:"
            f"{logical_date.isoformat()}"
        ),
    ).hex

    return RawRunContext(
        source=source,
        logical_date=logical_date,
        run_id=run_id,
        attempt_id=uuid4().hex,
        started_at=datetime.now(UTC),
    )


def build_run_prefix(
    context: RawRunContext,
) -> str:
    return (
        f"{context.source}/"
        f"extract_date={context.logical_date.isoformat()}/"
        f"run_id={context.run_id}"
    )


def build_attempt_prefix(
    context: RawRunContext,
) -> str:
    return (
        f"{build_run_prefix(context)}/"
        f"attempt_id={context.attempt_id}"
    )


def build_part_key(
    context: RawRunContext,
    part_number: int,
) -> str:
    return (
        f"{build_attempt_prefix(context)}/"
        f"part-{part_number:05d}.parquet"
    )


def build_manifest_key(
    context: RawRunContext,
) -> str:
    return (
        f"{build_attempt_prefix(context)}/"
        "manifest.json"
    )


def build_success_key(
    context: RawRunContext,
) -> str:
    return (
        f"{build_run_prefix(context)}/"
        "_SUCCESS.json"
    )


def build_manifest_payload(
    context: RawRunContext,
    objects: list[RawObjectRecord],
    completed_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source": context.source,
        "logical_date": context.logical_date.isoformat(),
        "run_id": context.run_id,
        "attempt_id": context.attempt_id,
        "started_at": context.started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "object_count": len(objects),
        "row_count": sum(
            obj.row_count
            for obj in objects
        ),
        "objects": [
            asdict(obj)
            for obj in objects
        ],
    }


def build_success_payload(
    context: RawRunContext,
    manifest_key: str,
    objects: list[RawObjectRecord],
    committed_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source": context.source,
        "logical_date": context.logical_date.isoformat(),
        "run_id": context.run_id,
        "attempt_id": context.attempt_id,
        "manifest_key": manifest_key,
        "object_count": len(objects),
        "row_count": sum(
            obj.row_count
            for obj in objects
        ),
        "committed_at": committed_at.isoformat(),
    }