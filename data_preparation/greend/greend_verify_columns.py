"""Verify whether column index i holds the same appliance type in every GREEND building.

Device IDs (ZigBee MACs) differ per building, so we profile every device by its
power distribution and daily usage pattern and compare devices across buildings.

Usage: python greend_verify_columns.py ~/data/GREEND_0-1_311014 [out.json]
"""
import glob
import io
import json
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import polars as pl

# Bin 0 holds exact zeros, then log-spaced bins from 0.5 W up to 10 kW, last bin is overflow.
BIN_EDGES = np.concatenate([[0.0, 1e-9], np.logspace(np.log10(0.5), 4, 40), [np.inf]])
ON_THRESHOLD_W = 5.0
MIN_VALID_TS = 1356998400  # 2013-01-01, earlier timestamps come from unsynced clocks
TZ_OFFSET_S = 3600  # Italy/Austria, CET (ignores DST, good enough for a usage signature)


def split_segments(raw):
    """Split a file into (header, body) segments, files may repeat or change header mid-file."""
    segments, header, body = [], None, []
    # Some files contain NUL padding (power loss while writing), e.g. building4/dataset_2014-02-03.csv.
    for line in raw.replace(b"\x00", b"").split(b"\n"):
        if line.startswith(b"timestamp,"):
            if header is not None and body:
                segments.append((header, body))
            header, body = line.rstrip(b"\r"), []
        elif line.strip():
            body.append(line)
    if header is not None and body:
        segments.append((header, body))
    return segments


def empty_stats():
    return {
        "n": 0, "nulls": 0, "on": 0, "sum": 0.0, "max": 0.0,
        "hist": np.zeros(len(BIN_EDGES) - 1, dtype=np.int64),
        "hour_sum": np.zeros(24), "hour_n": np.zeros(24, dtype=np.int64),
    }


def profile_file(path):
    with open(path, "rb") as f:
        raw = f.read()
    stats, headers, ragged = {}, Counter(), 0
    for header, body in split_segments(raw):
        names = header.decode().split(",")
        headers[tuple(names[1:])] += 1
        n_fields = len(names)
        good = [l for l in body if l.count(b",") + 1 == n_fields]
        ragged += len(body) - len(good)
        if not good:
            continue
        df = pl.read_csv(
            io.BytesIO(header + b"\n" + b"\n".join(good)),
            null_values="NULL",
            schema={n: pl.Float64 for n in names},
        )
        ts = df["timestamp"].to_numpy()
        valid_ts = ts >= MIN_VALID_TS
        hours = (((ts + TZ_OFFSET_S) // 3600) % 24).astype(np.int64)
        for dev in names[1:]:
            col = df[dev].to_numpy()
            s = stats.setdefault(dev, empty_stats())
            notnull = ~np.isnan(col)
            v = col[notnull]
            s["n"] += len(col)
            s["nulls"] += int((~notnull).sum())
            s["on"] += int((v > ON_THRESHOLD_W).sum())
            s["sum"] += float(v.sum())
            if len(v):
                s["max"] = max(s["max"], float(v.max()))
            s["hist"] += np.histogram(v, bins=BIN_EDGES)[0]
            m = notnull & valid_ts
            s["hour_sum"] += np.bincount(hours[m], weights=col[m], minlength=24)
            s["hour_n"] += np.bincount(hours[m], minlength=24)
    return path, stats, headers, ragged


def merge(dst, src):
    for dev, s in src.items():
        d = dst.setdefault(dev, empty_stats())
        for k in ("n", "nulls", "on", "sum", "hist", "hour_sum", "hour_n"):
            d[k] += s[k]
        d["max"] = max(d["max"], s["max"])


def quantile_from_hist(hist, q):
    """Approximate quantile (geometric bin centre) of the on-values only."""
    h = hist.copy()
    on_start = np.searchsorted(BIN_EDGES, ON_THRESHOLD_W) - 1
    h[: on_start] = 0
    if h.sum() == 0:
        return 0.0
    i = int(np.searchsorted(np.cumsum(h), q * h.sum()))
    lo, hi = BIN_EDGES[i], BIN_EDGES[i + 1]
    return float(np.sqrt(max(lo, 1e-3) * hi)) if np.isfinite(hi) else float(lo)


def summarize(s):
    nn = s["n"] - s["nulls"]
    hour_mean = np.divide(s["hour_sum"], s["hour_n"], out=np.zeros(24), where=s["hour_n"] > 0)
    return {
        "rows": s["n"],
        "null_pct": 100 * s["nulls"] / max(s["n"], 1),
        "on_pct": 100 * s["on"] / max(nn, 1),
        "mean_w": s["sum"] / max(nn, 1),
        "on_p50_w": quantile_from_hist(s["hist"], 0.5),
        "on_p95_w": quantile_from_hist(s["hist"], 0.95),
        "max_w": s["max"],
        "peak_hour": int(np.argmax(hour_mean)),
        "hist": s["hist"].tolist(),
        "hour_mean": hour_mean.tolist(),
    }


def signature(summary):
    """Vector used to compare devices: on-power distribution + normalised daily profile."""
    h = np.array(summary["hist"], dtype=float)
    on_start = np.searchsorted(BIN_EDGES, ON_THRESHOLD_W) - 1
    h = h[on_start:]
    h = h / h.sum() if h.sum() else h
    p = np.array(summary["hour_mean"])
    p = p / p.sum() if p.sum() else p
    return np.concatenate([h, p])


def js_distance(a, b):
    m = (a + b) / 2
    def kl(x, y):
        mask = x > 0
        return np.sum(x[mask] * np.log2(x[mask] / y[mask]))
    return float(np.sqrt(max(0.5 * kl(a, m) + 0.5 * kl(b, m), 0.0)))


def device_distance(sa, sb):
    a, b = signature(sa), signature(sb)
    nb = len(BIN_EDGES) - 1 - (np.searchsorted(BIN_EDGES, ON_THRESHOLD_W) - 1)
    return 0.5 * js_distance(a[:nb], b[:nb]) + 0.5 * js_distance(a[nb:], b[nb:])


def main(root, out_json):
    buildings = sorted(d for d in os.listdir(root) if d.startswith("building"))
    report = {}
    with ProcessPoolExecutor() as pool:
        for b in buildings:
            files = sorted(glob.glob(os.path.join(root, b, "*.csv")))
            stats, headers, ragged = {}, Counter(), 0
            for _, s, h, r in pool.map(profile_file, files, chunksize=4):
                merge(stats, s)
                headers.update(h)
                ragged += r
            # Canonical order = most frequent header, devices that appear only in other headers go last.
            canonical = list(headers.most_common(1)[0][0])
            extra = [d for h in headers for d in h if d not in canonical]
            order = canonical + sorted(set(extra), key=extra.index)
            report[b] = {
                "headers": [{"segments": c, "devices": list(h)} for h, c in headers.items()],
                "ragged_rows_skipped": ragged,
                "devices": [{"index": i, "id": d, **summarize(stats[d])} for i, d in enumerate(order)],
            }
            print(f"{b}: {len(files)} files, {len(order)} devices, {ragged} ragged rows skipped", flush=True)

    with open(out_json, "w") as f:
        json.dump(report, f)

    # Side-by-side table per column index.
    max_idx = max(len(r["devices"]) for r in report.values())
    for i in range(max_idx):
        print(f"\n=== column index {i} ===")
        print(f"{'building':<10} {'device':<18} {'null%':>6} {'on%':>6} {'mean W':>8} {'on p50':>8} {'on p95':>8} {'max W':>8} {'peak h':>6}")
        for b, r in report.items():
            if i >= len(r["devices"]):
                print(f"{b:<10} (no column)")
                continue
            d = r["devices"][i]
            print(f"{b:<10} {d['id']:<18} {d['null_pct']:6.1f} {d['on_pct']:6.1f} {d['mean_w']:8.1f} "
                  f"{d['on_p50_w']:8.0f} {d['on_p95_w']:8.0f} {d['max_w']:8.0f} {d['peak_hour']:6d}")

    # Nearest-neighbour test: is the most similar device in another building at the same index?
    devs = [(b, d) for b, r in report.items() for d in r["devices"]]
    same_idx_hits, total, intra_idx, inter_idx = 0, 0, [], []
    for b, d in devs:
        others = [(ob, od) for ob, od in devs if ob != b]
        dists = [(device_distance(d, od), ob, od) for ob, od in others]
        for dist, _, od in dists:
            (intra_idx if od["index"] == d["index"] else inter_idx).append(dist)
        best = min(dists, key=lambda x: x[0])
        total += 1
        same_idx_hits += best[2]["index"] == d["index"]
    n_idx = max_idx
    print(f"\nNearest neighbour in another building has the same column index: "
          f"{same_idx_hits}/{total} ({100 * same_idx_hits / total:.0f}%), chance level ~{100 / n_idx:.0f}%")
    print(f"Mean distance, same index: {np.mean(intra_idx):.3f}  different index: {np.mean(inter_idx):.3f}")


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        print(f"Usage: {sys.argv[0]} /path/to/GREEND_root [out.json]")
        sys.exit(-1)
    main(os.path.expanduser(sys.argv[1]), sys.argv[2] if len(sys.argv) == 3 else "greend_device_profiles.json")
