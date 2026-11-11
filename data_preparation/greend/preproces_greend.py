"""Write GREEND as a single long-format Parquet file: building_id, device, timestamp, measurement.

- Columns are matched by device ID (headers differ between and within files) and renamed to the
  GREEND paper's appliance names (greend_appliances.DEVICES).
- Devices that are 100% null or not documented in the paper are skipped.
- Exact duplicate records are removed; everything else is kept, including NULL measurements
  of the kept devices and the rows stamped 2000-01-01 by an unsynced clock.
- Rows are sorted by building_id, device, timestamp.

Usage: python preproces_greend.py ~/data/GREEND_0-1_311014 /path/to/greend.parquet
"""
import glob
import io
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from greend_appliances import DEVICES
from greend_verify_columns import split_segments

SCHEMA = pa.schema([
    ("building_id", pa.string()),
    ("device", pa.string()),
    ("timestamp", pa.timestamp('us')),
    ("measurement", pa.float32()),
])
ROW_GROUP_SIZE = 1_000_000


def read_file(path, names):
    """Parse one CSV into long format, keeping only the devices in `names` (device id -> name)."""
    with open(path, "rb") as f:
        raw = f.read()
    parts, ragged = [], 0
    device_type = pl.Enum(sorted(names.values()))
    for header, body in split_segments(raw):
        cols = header.decode().split(",")
        good = [l for l in body if l.count(b",") + 1 == len(cols)]
        ragged += len(body) - len(good)
        keep = [c for c in cols[1:] if c in names]
        if not good or not keep:
            continue
        df = pl.read_csv(
            io.BytesIO(header + b"\n" + b"\n".join(good)),
            null_values="NULL",
            schema={c: pl.Float64 for c in cols},
        )
        parts.append(
            df.select("timestamp", *keep)
            .unpivot(index="timestamp", variable_name="device", value_name="measurement")
            .with_columns(
                pl.col("device").replace_strict(names, return_dtype=device_type),
                (pl.col("timestamp") * 1e6).round().cast(pl.Int64).cast(pl.Datetime("us", "UTC")),
            )
        )
    return (pl.concat(parts) if parts else None), ragged


def main(root, out_path):
    buildings = sorted(d for d in os.listdir(root) if d.startswith("building"))
    with ProcessPoolExecutor() as pool, pq.ParquetWriter(out_path, SCHEMA, compression="zstd") as writer:
        for b in buildings:
            names = {dev_id: name for dev_id, name, _ in DEVICES[b] if name is not None}
            files = sorted(glob.glob(os.path.join(root, b, "*.csv")))
            results = list(pool.map(partial(read_file, names=names), files, chunksize=4))
            df = pl.concat([d for d, _ in results if d is not None])
            ragged = sum(r for _, r in results)

            parsed = df.height
            df = df.unique(maintain_order=True).sort("device", "timestamp", maintain_order=True)
            df = df.select(pl.lit(int(b.removeprefix("building")), pl.Int8).alias("building_id"), "device", "timestamp", "measurement")

            for batch in df.iter_slices(ROW_GROUP_SIZE):
                writer.write_table(batch.to_arrow().cast(SCHEMA), row_group_size=ROW_GROUP_SIZE)
            print(f"{b}: {len(names)} devices, {df.height:,} records written "
                  f"({df['measurement'].null_count():,} null), {parsed - df.height:,} exact duplicates removed, "
                  f"{ragged} malformed CSV rows skipped", flush=True)
            del df, results


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} /path/to/GREEND_root /path/to/output.parquet")
        sys.exit(-1)
    main(os.path.expanduser(sys.argv[1]), os.path.expanduser(sys.argv[2]))
