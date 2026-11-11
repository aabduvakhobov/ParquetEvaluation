"""Check GREEND timestamps: uniqueness/order within each building, sampling interval, and overlap across buildings.

Usage: python greend_check_timestamps.py ~/data/GREEND_0-1_311014
"""
import glob
import os
import sys
from concurrent.futures import ProcessPoolExecutor

import numpy as np

from greend_verify_columns import MIN_VALID_TS, split_segments


def file_timestamps(path):
    with open(path, "rb") as f:
        raw = f.read()
    n_fields_ok = []
    for header, body in split_segments(raw):
        n_fields = header.count(b",") + 1
        n_fields_ok += [l.split(b",", 1)[0] for l in body if l.count(b",") + 1 == n_fields]
    return np.array(n_fields_ok, dtype=np.float64)


def fmt(ts):
    return np.datetime_as_string(np.datetime64(int(ts), "s"))


def main(root):
    buildings = sorted(d for d in os.listdir(root) if d.startswith("building"))
    per_building = {}
    with ProcessPoolExecutor() as pool:
        for b in buildings:
            files = sorted(glob.glob(os.path.join(root, b, "*.csv")))
            parts = list(pool.map(file_timestamps, files, chunksize=4))
            ts = np.concatenate(parts)
            in_order = np.concatenate([p for p in parts if len(p)])
            backwards = int((np.diff(in_order) < 0).sum())
            valid = ts[ts >= MIN_VALID_TS]
            uniq = np.unique(valid)
            gaps = np.diff(uniq)
            per_building[b] = uniq
            print(f"\n== {b}: {len(ts):,} rows, {len(ts) - len(valid):,} with pre-2013 timestamps")
            print(f"   range {fmt(uniq[0])} .. {fmt(uniq[-1])}")
            print(f"   duplicate timestamps: {len(valid) - len(uniq):,}, backwards steps in file order: {backwards:,}")
            print(f"   interval p50 {np.median(gaps):.3f}s, p90 {np.percentile(gaps, 90):.3f}s, "
                  f"p99 {np.percentile(gaps, 99):.1f}s, gaps > 60s: {(gaps > 60).sum():,}")
            whole_seconds = np.unique(np.floor(valid))
            print(f"   rows sharing a whole second with another row: {len(valid) - len(whole_seconds):,}")

    print("\n== cross-building overlap")
    names = list(per_building)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            ta, tb = per_building[a], per_building[b]
            lo, hi = max(ta[0], tb[0]), min(ta[-1], tb[-1])
            exact = len(np.intersect1d(ta, tb))
            same_sec = len(np.intersect1d(np.floor(ta), np.floor(tb)))
            if hi > lo:
                print(f"{a}-{b}: overlap {(hi - lo) / 86400:.0f} days, exact ts matches {exact:,}, same-second matches {same_sec:,}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} /path/to/GREEND_root")
        sys.exit(-1)
    main(os.path.expanduser(sys.argv[1]))
