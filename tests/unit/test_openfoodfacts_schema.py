import pyarrow as pa
import pytest

from pipeline.dq.openfoodfacts import (
    DataQualityError,
    validate_openfoodfacts_table,
)
from pipeline.schemas.openfoodfacts import (
    OPENFOODFACTS_SCHEMA,
)
from pipeline.schemas.validation import (
    records_to_table,
)


def test_valid_records_create_expected_table() -> None:
    records = [
        {
            "code": "12345",
            "product_name": "Test Juice",
            "brands": "Test Brand",
            "nutrition_grades": "b",
            "categories_tags_en": [
                "Beverages",
                "Juices",
            ],
            "last_modified_t": 1700000000,
        }
    ]

    table = records_to_table(
        records=records,
        schema=OPENFOODFACTS_SCHEMA
    )

    assert table.num_rows == 1
    assert table.schema == OPENFOODFACTS_SCHEMA

def test_duplicate_codes_fail_dq() -> None:
    records = [
        {
            "code": "12345",
            "product_name": "Product One",
            "last_modified_t": 1700000000,
        },
        {
            "code": "12345",
            "product_name": "Product Two",
            "last_modified_t": 1700000001,
        },
    ]

    table = pa.Table.from_pylist(
        records,
        schema=OPENFOODFACTS_SCHEMA,
    )

    with pytest.raises(
        DataQualityError,
        match="Duplicate product codes"
    ):
        validate_openfoodfacts_table(table)