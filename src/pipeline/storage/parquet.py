from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(
        table: pa.Table,
        output_path: Path,
) -> Path:
    """Write a PyArrow table to a compressed Parquet file."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    pq.write_table(
        table,
        output_path,
        compression="snappy"
    )

    return output_path

def read_parquet_bytes(
    data: bytes,
) -> pa.Table:
    """
    Read Parquet bytes into a PyArrow table.
    """

    buffer = pa.BufferReader(data)

    return pq.read_table(buffer)