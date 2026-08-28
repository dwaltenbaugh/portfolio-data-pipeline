from typing import Any

import pyarrow as pa


class SchemaValidationError(Exception):
    """Raised when records cannot conform to an expected schema."""

def records_to_table(
        records: list[dict[str, Any]],
        schema: pa.Schema,
) -> pa.Table:
    """Convert extracted records to a PyArrow table using an explicit schema."""

    if not records:
        raise SchemaValidationError(
            "Cannot create an Arrow table from an empty record set."
        )

    try:
        table = pa.Table.from_pylist(
            records,
            schema=schema,
        )
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise SchemaValidationError(
            f"Records failed schema validation: {exc}"
        ) from exc

    return table