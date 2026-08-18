import pyarrow as pa
import pyarrow.compute as pc


class DataQualityError(Exception):
    """Raised when data fails a blocking quality check."""


def validate_openfoodfacts_table(
        table: pa.Table,
) -> None:
    """Run blocking raw-layer checks on Open Food Facts data."""

    if table.num_rows == 0:
        raise DataQualityError(
            "Open Food Facts contains zero rows."
        )

    null_code_count = pc.sum(
        pc.is_null(table["code"])
    ).as_py()

    if null_code_count > 0:
        raise DataQualityError(
            f"Found {null_code_count} records with null product codes."
        )

    unique_code_count = pc.count_distinct(
        table["code"]
    ).as_py()

    if unique_code_count != table.num_rows:
        raise DataQualityError(
            "Duplicate product codes detected within page."
        )