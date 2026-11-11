"""Rewrite the long-format GREEND Parquet (building_id, device, timestamp, measurement) as a
multivariate time series: building_id, timestamp, <appliance>, <appliance>2, ...

- Columns are appliance types (greend_appliances.DEVICES), shared across buildings: every building's
  TV goes into `tv`. A building's second device of the same type goes into `tv2`, the third into
  `tv3`, and so on, in DEVICES column order.
- Each building's devices are joined on timestamp; a device that has no value at that timestamp,
  or that the building does not have, is NULL.
- Rows are sorted by building_id, timestamp.
- Memory: one building is processed at a time (the input is sorted by building, one building per row group).

Usage: python greend_to_wide.py ~/data/GREEND/greend.parquet ~/data/GREEND/greend_wide.parquet
"""
import os
import sys
from collections import Counter, defaultdict

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from greend_appliances import DEVICES

ROW_GROUP_SIZE = 1_000_000


def slot_columns():
    """building id -> {device name -> slot column}, and all slot columns in output order."""
    slots, order = {}, []
    for b, devices in DEVICES.items():
        seen = Counter()
        slots[b.removeprefix("building")] = names = {}
        for _, name, appliance in devices:
            if name is None:
                continue
            seen[appliance] += 1
            col = appliance if seen[appliance] == 1 else f"{appliance}{seen[appliance]}"
            names[name] = col
            if col not in order:
                order.append(col)
    # Keep tv, tv2, tv3 next to each other.
    first = {c: i for i, c in enumerate(order)}
    order.sort(key=lambda c: (first[c.rstrip("0123456789")], c))
    return slots, order


def main(in_path, out_path):
    slots, columns = slot_columns()
    schema = pa.schema([("building_id", pa.string()), ("timestamp", pa.timestamp("us"))]
                       + [(c, pa.float32()) for c in columns])

    f = pq.ParquetFile(in_path, read_dictionary=["device"])
    row_groups = defaultdict(list)
    for i in range(f.metadata.num_row_groups):
        stats = f.metadata.row_group(i).column(0).statistics
        assert stats.min == stats.max, f"row group {i} spans several buildings"
        row_groups[stats.min].append(i)

    with pq.ParquetWriter(out_path, schema, compression="zstd") as writer:
        for b in sorted(row_groups, key=int):
            long = pl.DataFrame(f.read_row_groups(row_groups[b], columns=["device", "timestamp", "measurement"]))
            names = slots[b]
            unknown = set(long["device"].unique().cast(pl.String)) - names.keys()
            assert not unknown, f"building {b}: devices missing from DEVICES: {unknown}"
            # Input is sorted by device, timestamp, so a repeat is always next to its twin.
            dups = long.select(((pl.col("device") == pl.col("device").shift())
                                & (pl.col("timestamp") == pl.col("timestamp").shift())).sum()).item()
            assert dups == 0, f"building {b}: {dups} rows with a repeated (device, timestamp)"

            wide = (long.pivot(on="device", index="timestamp", values="measurement")
                    .rename(names, strict=False)
                    .sort("timestamp"))
            del long
            wide = wide.select(
                pl.lit(b).alias("building_id"),
                "timestamp",
                *[pl.col(c) if c in wide.columns else pl.lit(None, pl.Float32).alias(c) for c in columns],
            )
            for batch in wide.iter_slices(ROW_GROUP_SIZE):
                writer.write_table(batch.to_arrow().cast(schema), row_group_size=ROW_GROUP_SIZE)
            print(f"building {b}: {wide.height:,} timestamps, devices -> "
                  + ", ".join(f"{n}->{c}" for n, c in names.items()), flush=True)
            del wide


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} /path/to/greend.parquet /path/to/greend_wide.parquet")
        sys.exit(-1)
    main(os.path.expanduser(sys.argv[1]), os.path.expanduser(sys.argv[2]))
