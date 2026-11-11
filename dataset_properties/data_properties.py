"""Data properties of the original datasets, to explain which Parquet value encoding wins.

We analyze by 1,048,576-row Row Groups in file order.
Basic properties (nulls, min/max/mean/std, percentiles, string lengths, timestamp deltas, ...) are computed for
every column chunk, and aggregated per column and per dataset; the schema comes from the Parquet footers.
For every float column chunk, the chunk is also written as a real Parquet column chunk (pyarrow, in memory) for
each value encoding x block compressor, so the measured sizes include pages, definition levels and dictionary
fallback. Source files are opened read-only.

Results are cached next to this file, one CSV per level (see COMPUTE_LEVEL); a recompute overwrites all three.
Column and dataset levels cover the sampled row groups only (SAMPLE_EVERY), except the footer-based schema,
file and row counts. Column-level percentiles come from a uniform SAMPLE_FRACTION sample of each column chunk.

Run: python -I data_properties.py
"""
import glob
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROWS_PER_ROW_GROUP = 1_048_576
DICT_PAGE_LIMITS = {'10K': 10 * 1024, '1M': 1024 * 1024}
HERE = os.path.dirname(os.path.abspath(__file__))
COMPUTE_LEVEL = {'column_chunk': os.path.join(HERE, 'data_properties.csv'),
          'column': os.path.join(HERE, 'data_properties_columns.csv'),
          'dataset': os.path.join(HERE, 'data_properties_datasets.csv')}
WORKERS = 24

PERCENTILES = [1, 5, 25, 50, 75, 95, 99]
PERCENTILE_NAMES = [f'p{p:02d}' for p in PERCENTILES]
SAMPLE_FRACTION = 0.01
TIME_UNIT_SECONDS = {'s': 1.0, 'ms': 1e-3, 'us': 1e-6, 'ns': 1e-9}

# The original datasets are read from <repo>/data/<dataset>/*.parquet (see the top-level README).
DATA_DIR = os.path.join(os.path.dirname(HERE), 'data')
DATASET_NAMES = ['blued', 'greend', 'redd', 'neon_wind']
ORIGINAL_DATASETS = {d: sorted(glob.glob(os.path.join(DATA_DIR, d, '*.parquet'))) for d in DATASET_NAMES}
# Keep every n-th row group of each file (1 = all row groups).
SAMPLE_EVERY = {'blued': 1, 'greend': 1, 'redd': 1, 'neon_wind': 1}

# Block compressors as (pyarrow name, level); levels match ones in the experiments.
COMPRESSORS = {'snappy': ('snappy', None), 'lz4': ('lz4', None), 'gzip9': ('gzip', 9),
               'zstd13': ('zstd', 13), 'brotli7': ('brotli', 7)}
# Value encodings as pyarrow writer options (dictionary falls back to PLAIN when the page limit is exceeded).
ENCODINGS = {'PLAIN': dict(use_dictionary=False),
             'BSS': dict(use_dictionary=False, column_encoding='BYTE_STREAM_SPLIT'),
             'DICT10K': dict(use_dictionary=True, dictionary_pagesize_limit=DICT_PAGE_LIMITS['10K']),
             'DICT1M': dict(use_dictionary=True, dictionary_pagesize_limit=DICT_PAGE_LIMITS['1M'])}

# Byte names in big-endian order: byte 0 holds sign + exponent, byte 3 is the least significant mantissa byte.
BYTE_NAMES = ['b0_sign_exp', 'b1_exp_mant', 'b2_mant', 'b3_mant_lsb']


def entropy_bits(counts):
    p = counts[counts > 0] / counts.sum()
    return float(-(p * np.log2(p)).sum())


def rows_before_dict_overflow(first_seen, width, limit):
    """Rows encoded with the dictionary before it exceeds `limit` bytes (writer then falls back)."""
    max_entries = limit // width
    return int(first_seen[max_entries]) if len(first_seen) > max_entries else None


def parquet_sizes(column):
    """Compressed size of `column` written as one Parquet column chunk, per encoding x compressor.

    Writer defaults otherwise, as in the experiments: 1 MiB data pages capped at 20,000 rows, statistics off.
    """
    table = pa.table({'v': column})
    sizes = {}
    for enc, enc_options in ENCODINGS.items():
        options = dict(enc_options)
        if 'column_encoding' in options:
            options['column_encoding'] = {'v': options['column_encoding']}
        for comp, (codec, level) in COMPRESSORS.items():
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf, compression=codec, compression_level=level, write_statistics=False,
                           row_group_size=ROWS_PER_ROW_GROUP, **options)
            md = pq.ParquetFile(pa.BufferReader(buf.getvalue())).metadata
            sizes[f'size_{comp}_{enc}'] = md.row_group(0).column(0).total_compressed_size
    return sizes


def float_properties(values):
    n = len(values)
    as_u32 = values.view(np.uint32)
    uniq, first_idx, counts = np.unique(as_u32, return_index=True, return_counts=True)
    first_seen = np.sort(first_idx)
    big_endian = values.astype('>f4').view(np.uint8).reshape(n, 4)
    row = {'non_null_values': n, 'distinct': len(uniq), 'cardinality_ratio': len(uniq) / n,
           'value_entropy': entropy_bits(counts),
           'repeat_ratio': float((as_u32[1:] == as_u32[:-1]).mean()) if n > 1 else np.nan}
    for i, name in enumerate(BYTE_NAMES):
        row[f'H_{name}'] = entropy_bits(np.bincount(big_endian[:, i], minlength=256))
    # Redundancy between the 4 bytes of a value: 0 when bytes are independent; BSS's split breaks it up.
    row['byte_redundancy'] = sum(row[f'H_{b}'] for b in BYTE_NAMES) - row['value_entropy']
    # Entropy left in the low byte(s) once the upper bytes are known: ~0 when few distinct values pin them down.
    row['H_b3_given_upper3'] = row['value_entropy'] - entropy_bits(np.unique(as_u32 >> 8, return_counts=True)[1])
    row['H_low2_given_high2'] = row['value_entropy'] - entropy_bits(np.unique(as_u32 >> 16, return_counts=True)[1])
    for name, limit in DICT_PAGE_LIMITS.items():
        overflow = rows_before_dict_overflow(first_seen, 4, limit)
        row[f'dict_fits_{name}'] = overflow is None
        row[f'dict_row_coverage_{name}'] = 1.0 if overflow is None else overflow / n
    return row


def timestamp_properties(values):
    n = len(values)
    uniq, first_idx = np.unique(values, return_index=True)
    first_seen = np.sort(first_idx)
    deltas = np.diff(values)
    _, delta_counts = np.unique(deltas, return_counts=True)
    row = {'non_null_values': n, 'distinct': len(uniq), 'cardinality_ratio': len(uniq) / n,
           'modal_delta_share': delta_counts.max() / len(deltas) if len(deltas) else np.nan}
    for name, limit in DICT_PAGE_LIMITS.items():
        overflow = rows_before_dict_overflow(first_seen, 8, limit)
        row[f'dict_fits_{name}'] = overflow is None
        row[f'dict_row_coverage_{name}'] = 1.0 if overflow is None else overflow / n
    return row


def kind_of(arrow_type):
    if pa.types.is_floating(arrow_type):
        return 'float'
    if pa.types.is_integer(arrow_type):
        return 'int'
    if pa.types.is_timestamp(arrow_type):
        return 'timestamp'
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return 'string'
    return 'bool' if pa.types.is_boolean(arrow_type) else 'other'


def numeric_basic(values, seed):
    """Basic stats over the finite values, plus a uniform sample of them for column-level percentiles."""
    v = values.astype(np.float64)
    finite = v[np.isfinite(v)]
    row = {'nan_count': int(np.isnan(v).sum()), 'inf_count': int(np.isinf(v).sum()), 'finite_values': len(finite)}
    if len(finite):
        row.update({'min': finite.min(), 'max': finite.max(), 'mean': finite.mean(), 'std': finite.std(),
                    'zero_ratio': float((finite == 0).mean())})
        row.update(zip(PERCENTILE_NAMES, np.percentile(finite, PERCENTILES)))
    m = int(np.ceil(len(finite) * SAMPLE_FRACTION))
    return row, np.random.default_rng(seed).choice(finite, m, replace=False)


def timestamp_basic(values, unit):
    """`values` are int64 in `unit`; deltas in seconds."""
    deltas = np.diff(values) * TIME_UNIT_SECONDS[unit]
    row = {'ts_min': pd.Timestamp(int(values.min()), unit=unit).isoformat(),
           'ts_max': pd.Timestamp(int(values.max()), unit=unit).isoformat()}
    if len(deltas):
        row.update({'delta_min_s': deltas.min(), 'delta_median_s': float(np.median(deltas)),
                    'delta_max_s': deltas.max(), 'nonpositive_delta_share': float((deltas <= 0).mean())})
    return row


def string_basic(values):
    """`values` is a non-null pyarrow string array; also returns its value counts for column-level aggregation."""
    vc = pc.value_counts(values)
    counts = vc.field('counts').to_numpy()
    lengths = pc.utf8_length(values).to_numpy()
    n = len(values)
    row = {'distinct': len(counts), 'cardinality_ratio': len(counts) / n, 'value_entropy': entropy_bits(counts),
           'top_value_share': counts.max() / n, 'len_min': int(lengths.min()), 'len_max': int(lengths.max()),
           'len_mean': float(lengths.mean())}
    return row, Counter(dict(zip(vc.field('values').to_pylist(), counts.tolist())))


def row_group_tasks(dataset):
    """(dataset, path, k) for every SAMPLE_EVERY-th re-cut row group k of each file."""
    for path in ORIGINAL_DATASETS[dataset]:
        with open(path, 'rb') as fh:
            num_rows = pq.ParquetFile(fh).metadata.num_rows
        for k in range(0, -(-num_rows // ROWS_PER_ROW_GROUP), SAMPLE_EVERY[dataset]):
            yield dataset, path, k


def read_row_group(path, k):
    """Rows [k * ROWS_PER_ROW_GROUP, (k + 1) * ROWS_PER_ROW_GROUP) of the file, reading only overlapping row groups."""
    start, stop = k * ROWS_PER_ROW_GROUP, (k + 1) * ROWS_PER_ROW_GROUP
    with open(path, 'rb') as fh:
        pf = pq.ParquetFile(fh)
        md, offset, groups, first = pf.metadata, 0, [], None
        for i in range(md.num_row_groups):
            n = md.row_group(i).num_rows
            if offset < stop and offset + n > start:
                groups.append(i)
                first = offset if first is None else first
            offset += n
        return pf.read_row_groups(groups).slice(start - first, ROWS_PER_ROW_GROUP)


def row_group_properties(task):
    """One row per column chunk, plus {column: percentile sample or string value counts} for aggregation."""
    dataset, path, k = task
    table = read_row_group(path, k)
    rows, samples = [], {}
    for field, col in zip(table.schema, table.columns):
        col = col.combine_chunks()
        non_null = col.drop_null()
        kind = kind_of(field.type)
        row = {'dataset': dataset, 'file': os.path.basename(path), 'row_group': k, 'column': field.name,
               'rows': table.num_rows, 'nulls': col.null_count, 'null_ratio': col.null_count / table.num_rows,
               'kind': kind, 'type': str(field.type), 'non_null_values': len(non_null)}
        if len(non_null) == 0:  # all-null column chunk: nothing to describe or encode
            rows.append(row)
            continue
        if kind == 'string':
            stats, samples[field.name] = string_basic(non_null)
            row.update(stats)
        elif kind == 'timestamp':
            values = non_null.to_numpy(zero_copy_only=False).astype('int64')
            row.update(timestamp_basic(values, field.type.unit))
            row.update(timestamp_properties(values))
        elif kind in ('float', 'int'):
            values = non_null.to_numpy(zero_copy_only=False)
            stats, samples[field.name] = numeric_basic(values, seed=k)
            row.update(stats)
            if pa.types.is_float32(field.type):
                row.update({**float_properties(values), **parquet_sizes(col)})
            else:
                row['distinct'] = len(np.unique(values))
                row['cardinality_ratio'] = row['distinct'] / len(values)
        elif kind == 'bool':
            row['true_ratio'] = pc.sum(non_null).as_py() / len(non_null)
        rows.append(row)
    return rows, samples


def footer_schema(dataset):
    """Footer-based (exact, unsampled) per-column schema and per-dataset file/row counts."""
    paths = ORIGINAL_DATASETS[dataset]
    total_rows, row_groups, schemas = 0, 0, set()
    for path in paths:
        with open(path, 'rb') as fh:
            pf = pq.ParquetFile(fh)
            md, schema = pf.metadata, pf.schema_arrow
            if path == paths[0]:
                first_md, first_schema = md, schema
        total_rows += md.num_rows
        row_groups += md.num_row_groups
        schemas.add(str(schema))
    columns = []
    for i, field in enumerate(first_schema):
        chunk = first_md.row_group(0).column(i)
        columns.append({'dataset': dataset, 'column': field.name, 'kind': kind_of(field.type),
                        'type': str(field.type), 'physical_type': first_md.schema.column(i).physical_type,
                        'nullable': field.nullable, 'source_compression': chunk.compression,
                        'source_encodings': ';'.join(chunk.encodings)})
    kinds = Counter(c['kind'] for c in columns)
    dataset_row = {'dataset': dataset, 'files': len(paths), 'total_rows': total_rows,
                   'file_bytes': sum(os.path.getsize(p) for p in paths), 'source_row_groups': row_groups,
                   'columns': len(columns), **{f'{k}_columns': kinds.get(k, 0) for k in ('float', 'int', 'timestamp', 'string', 'bool', 'other')},
                   'schema': '; '.join(f"{c['column']}:{c['type']}" for c in columns),
                   'schema_consistent': len(schemas) == 1, 'sample_every': SAMPLE_EVERY[dataset]}
    return columns, dataset_row


def weighted_mean(x, w):
    return (x.fillna(0) * w).sum() / w.sum() if w.sum() else np.nan


def column_level(chunks, samples, schema_columns):
    """Aggregates column chunks per column: counts and min/max exact, mean/std pooled, percentiles from samples."""
    rows = []
    for info in schema_columns:
        g = chunks[(chunks.dataset == info['dataset']) & (chunks.column == info['column'])]
        rows_total, nulls = g.rows.sum(), g.nulls.sum()
        row = {**info, 'column_chunks': len(g), 'rows': rows_total, 'nulls': nulls,
               'null_ratio': nulls / rows_total if rows_total else np.nan, 'non_null_values': g.non_null_values.sum()}
        sample = samples.get((info['dataset'], info['column']))
        if info['kind'] in ('float', 'int'):
            n = g.finite_values.fillna(0)
            row.update({'nan_count': g.nan_count.sum(), 'inf_count': g.inf_count.sum(), 'finite_values': n.sum(),
                        'distinct_per_chunk_median': g.distinct.median()})
            if n.sum():
                mean = weighted_mean(g['mean'], n)
                var = weighted_mean(g['std'] ** 2 + (g['mean'] - mean) ** 2, n)
                row.update({'min': g['min'].min(), 'max': g['max'].max(), 'mean': mean, 'std': np.sqrt(var),
                            'zero_ratio': weighted_mean(g.zero_ratio, n)})
                row.update(zip(PERCENTILE_NAMES, np.percentile(np.concatenate(sample), PERCENTILES)))
        elif info['kind'] == 'timestamp':
            row.update({'ts_min': g.ts_min.min(), 'ts_max': g.ts_max.max(), 'delta_min_s': g.delta_min_s.min(),
                        'delta_median_s': g.delta_median_s.median(), 'delta_max_s': g.delta_max_s.max(),
                        'nonpositive_delta_share': weighted_mean(g.nonpositive_delta_share, g.non_null_values),
                        'distinct_per_chunk_median': g.distinct.median()})
        elif info['kind'] == 'string' and sample:
            counts = np.array(list(sample.values()))
            row.update({'distinct': len(counts), 'cardinality_ratio': len(counts) / counts.sum(),
                        'value_entropy': entropy_bits(counts), 'top_value_share': counts.max() / counts.sum(),
                        'len_min': g.len_min.min(), 'len_max': g.len_max.max(),
                        'len_mean': weighted_mean(g.len_mean, g.non_null_values),
                        'distinct_per_chunk_median': g.distinct.median()})
        elif info['kind'] == 'bool':
            row['true_ratio'] = weighted_mean(g.true_ratio, g.non_null_values)
        rows.append(row)
    return pd.DataFrame(rows)


def dataset_level(chunks, columns, dataset_rows):
    rows = []
    for row in dataset_rows:
        g, c = chunks[chunks.dataset == row['dataset']], columns[columns.dataset == row['dataset']]
        cells = c.rows.sum()
        rows.append({**row, 'measured_row_groups': g[['file', 'row_group']].drop_duplicates().shape[0],
                     'sampled_rows': g.drop_duplicates(['file', 'row_group']).rows.sum(),
                     'null_ratio': c.nulls.sum() / cells if cells else np.nan,
                     'columns_with_nulls': int((c.nulls > 0).sum())})
    return pd.DataFrame(rows)


def load(level='column_chunk', recompute=False):
    """Cached properties at `level` ('column_chunk', 'column' or 'dataset'); recompute rewrites all levels."""
    if os.path.exists(COMPUTE_LEVEL[level]) and not recompute:
        return pd.read_csv(COMPUTE_LEVEL[level])
    missing = [os.path.join(DATA_DIR, d) for d, paths in ORIGINAL_DATASETS.items() if not paths]
    if missing:
        raise FileNotFoundError('No Parquet files in: ' + ', '.join(missing))
    tasks = [t for d in ORIGINAL_DATASETS for t in row_group_tasks(d)]
    rows, samples = [], {}
    with ProcessPoolExecutor(WORKERS) as pool:
        for i, ((dataset, _, _), (result, chunk_samples)) in enumerate(
                zip(tasks, pool.map(row_group_properties, tasks, chunksize=1))):
            rows.extend(result)
            for column, sample in chunk_samples.items():
                if isinstance(sample, Counter):
                    samples.setdefault((dataset, column), Counter()).update(sample)
                else:
                    samples.setdefault((dataset, column), []).append(sample)
            if i % 50 == 0:
                print(f'{i + 1}/{len(tasks)} row groups', flush=True)
    schema_columns, dataset_rows = [], []
    for d in ORIGINAL_DATASETS:
        cols, drow = footer_schema(d)
        schema_columns.extend(cols)
        dataset_rows.append(drow)
    results = {'column_chunk': pd.DataFrame(rows)}
    results['column'] = column_level(results['column_chunk'], samples, schema_columns)
    results['dataset'] = dataset_level(results['column_chunk'], results['column'], dataset_rows)
    for name, df in results.items():
        df.to_csv(COMPUTE_LEVEL[name], index=False)
    return results[level]


if __name__ == '__main__':
    load(recompute=True)
