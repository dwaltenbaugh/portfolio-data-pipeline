import pyarrow as pa

OPENFOODFACTS_SCHEMA = pa.schema(
    [
        pa.field("code", pa.string(), nullable=False),
        pa.field("product_name", pa.string()),
        pa.field("brands", pa.string()),
        pa.field("nutrition_grades", pa.string()),
        pa.field(
            "categories_tags_en",
            pa.list_(pa.string()),
        ),
        pa.field("last_modified_t", pa.int64()),
    ]
)