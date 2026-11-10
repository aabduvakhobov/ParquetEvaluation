"""Find the Pareto-optimal Parquet configurations w.r.t. storage size, ingestion time and query time.

Reads the experiment results (strictly read-only) from the per-dataset SQLite databases. Every
output file is one configuration; all three metrics are minimized:

- storage_size: file_size_in_bytes (median over re-runs of the same output file)
- ingestion_time: compression_time_in_secs (median over re-runs)
- query_time: per system × column × interval, the median execution time over the file's queries
  (query_runs.execution_time_ms holds microseconds despite its name),
  normalized by the fastest file; the geometric mean over these streams is the query score
  (1.0 = fastest everywhere). Files with different row group sizes run different (random) queries
  of the same intervals, so they are compared via the median. Only streams run on at least
  `--min-coverage` of the files are scored, and files missing any of them are dropped. `--system`
  scores the queries of one system only (e.g. duckdb or datafusion); by default all are combined.

`pareto_points` returns all Pareto-optimal configurations; `closest_to_ideal` prunes them to the
single one closest to the ideal point (1, 1, 1) of the normalized metrics (metric / best metric on
the dataset), i.e. with the smallest (weighted) geometric mean of the three. `--weights` favours a
metric, e.g. `storage_size=0.4 ingestion_time=0.3 query_time=0.3`; the default weighs all three equally. Weights only
change which Pareto point is chosen, not the Pareto front itself.

Usage: python compute_pareto.py [--datasets blued redd] [--results-dir data/output] [--error-bound 0] [--out-dir pareto_results]
                                [--weights size=0.4 ingestion=0.3 query=0.3] [--top-n 1]
                                [--system duckdb]
"""
import argparse
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd


DATASETS = ['blued', 'greend', 'redd', 'neon_wind']


DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent.parent / 'data' / 'output'


def get_path_to_dataset(results_dir=None):
    """Per-dataset result databases of test_parameters_in_isolation.py: <results_dir>/<dataset>.db.

    `results_dir` defaults to <repo>/data/output.
    """
    results_dir = Path(results_dir) if results_dir is not None else DEFAULT_RESULTS_DIR
    return {name: str(results_dir / f'{name}.db') for name in DATASETS}


# Knobs shown next to each configuration in the output
CONFIG_COLUMNS = ['compression', 'value_encoding', 'timestamp_encoding', 'max_number_of_rows_in_row_group',
                  'statistics_enabled', 'dictionary_enabled', 'dictionary_page_size_limit',
                  'data_page_size_limit', 'bloom_filter_enabled', 'offset_index_disabled']
METRICS = ['size', 'ingestion', 'query']
STREAM = ['system_name', 'column_name', 'interval']


def connect_read_only(path):
    return sqlite3.connect(f'file:{path}?mode=ro', uri=True)


def load_dataset(db_path, error_bound, system=None):
    """Return (file-level df, query-level df) for the files written with `error_bound`. Only queries
    run on `system` (e.g. 'duckdb' or 'datafusion') are returned; `None` returns those of all systems."""
    file_sql = f"""SELECT output_file_name, {', '.join(CONFIG_COLUMNS)}, file_size_in_bytes, compression_time_in_secs
                   FROM dataset_struct_properties WHERE error_bound = ?"""
    query_sql = """SELECT qr.output_file_name, qr.system_name, qr.column_name, q.interval, qr.execution_time_ms
                   FROM query_runs qr
                   JOIN query q ON q.id = qr.query_id
                   WHERE qr.output_file_name IN
                       (SELECT output_file_name FROM dataset_struct_properties WHERE error_bound = ?)"""
    query_params = (error_bound,)
    if system is not None:
        query_sql += ' AND qr.system_name = ?'
        query_params += (system,)
    con = connect_read_only(db_path)
    try:
        files = pd.read_sql_query(file_sql, con, params=(error_bound,))
        queries = pd.read_sql_query(query_sql, con, params=query_params)
    finally:
        con.close()
    if system is not None and queries.empty:
        raise ValueError(f"No query runs for system '{system}' in {db_path}")
    return files, queries


def geomean(x):
    return float(np.exp(np.log(np.asarray(x, dtype=float)).mean()))


def query_scores(queries, min_coverage):
    """Query score per output file (see module docstring); also returns the scored streams."""
    per_stream = queries.groupby(['output_file_name'] + STREAM, as_index=False).execution_time_ms.median()
    n_files = per_stream.output_file_name.nunique()
    coverage = per_stream.groupby(STREAM).output_file_name.nunique() / n_files
    streams = coverage[coverage >= min_coverage].index
    per_stream = per_stream.set_index(STREAM).loc[lambda df: df.index.isin(streams)].reset_index()
    complete = per_stream.groupby('output_file_name').size() == len(streams)
    per_stream = per_stream[per_stream.output_file_name.isin(complete[complete].index)]
    per_stream['norm'] = per_stream.execution_time_ms / per_stream.groupby(STREAM).execution_time_ms.transform('min')
    scores = per_stream.groupby('output_file_name').agg(
        query=('norm', geomean), mean_query_time_us=('execution_time_ms', 'mean'))
    return scores, streams


def pareto_mask(values):
    """Boolean mask of non-dominated rows (minimization in every column)."""
    values = np.asarray(values, dtype=float)
    mask = np.ones(len(values), dtype=bool)
    for i, v in enumerate(values):
        dominated_by = np.all(values <= v, axis=1) & np.any(values < v, axis=1)
        mask[i] = not dominated_by.any()
    return mask


def compute_metrics(db_path, error_bound='0', min_coverage=0.9, system=None):
    """One row per configuration (output file) with raw and normalized size, ingestion and query metrics.
    The query metric uses only the queries run on `system`; `None` combines all systems (DuckDB and DataFusion)."""
    files, queries = load_dataset(db_path, error_bound, system)
    # Re-runs of the same output file are aggregated by the median
    metrics = files.groupby('output_file_name').agg(
        **{c: (c, 'first') for c in CONFIG_COLUMNS},
        size=('file_size_in_bytes', 'median'),
        ingestion=('compression_time_in_secs', 'median'),
        n_runs=('output_file_name', 'size'))
    scores, streams = query_scores(queries, min_coverage)
    dropped = metrics.index.difference(scores.index)
    metrics = metrics.join(scores, how='inner').reset_index()
    for m in METRICS:
        metrics[f'{m}_norm'] = metrics[m] / metrics[m].min()
    return metrics, streams, dropped


def pareto_points(metrics):
    """All Pareto-optimal configurations w.r.t. size, ingestion and query, sorted by size."""
    return metrics[pareto_mask(metrics[METRICS])].sort_values('size')


def normalize_weights(weights=None):
    """Weights per metric summing to 1; `None` weighs all metrics equally, missing metrics get 0."""
    if weights is None:
        weights = {m: 1.0 for m in METRICS}
    unknown = set(weights) - set(METRICS)
    if unknown:
        raise ValueError(f'Unknown metrics in weights: {sorted(unknown)} (expected a subset of {METRICS})')
    w = pd.Series({m: float(weights.get(m, 0.0)) for m in METRICS})
    if (w < 0).any() or w.sum() <= 0:
        raise ValueError(f'Weights must be non-negative with a positive sum, got {weights}')
    return w / w.sum()


def closest_to_ideal(points, n=1, weights=None):
    """Prune Pareto points to the `n` closest to the ideal point (1, 1, 1) of the normalized metrics,
    i.e. with the smallest weighted geometric mean of the three, closest first. `weights` maps
    metric -> weight (normalized to sum to 1); `None` weighs them equally."""
    w = normalize_weights(weights)
    log_norm = np.log(points[[f'{m}_norm' for m in METRICS]])
    distance = np.exp((log_norm * w.rename(lambda m: f'{m}_norm')).sum(axis=1))
    return points.assign(ideal_distance=distance, **{f'w_{m}': w[m] for m in METRICS}).nsmallest(n, 'ideal_distance')


def parse_weights(values):
    """Parse ['size=0.4', 'query=0.3', ...] into {'size': 0.4, 'query': 0.3, ...}."""
    weights = {}
    for value in values:
        metric, sep, weight = value.partition('=')
        if not sep:
            raise argparse.ArgumentTypeError(f"Expected metric=weight, got '{value}'")
        weights[metric] = float(weight)
    return weights


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--datasets', nargs='+', choices=DATASETS, default=DATASETS)
    parser.add_argument('--results-dir', type=Path,
                        help='directory with <dataset>.db (default: <repo>/data/output)')
    parser.add_argument('--error-bound', default='0', help="error_bound value to keep (default: '0', lossless)")
    parser.add_argument('--min-coverage', type=float, default=0.9,
                        help='score only query streams run on at least this fraction of the files')
    parser.add_argument('--out-dir', type=Path, default=Path(__file__).parent / 'pareto_results')
    parser.add_argument('--weights', nargs='+', metavar='METRIC=WEIGHT',
                        help=f'weights of {METRICS} when choosing among Pareto points (default: equal)')
    parser.add_argument('--top-n', type=int, default=1, help='number of chosen Pareto points per dataset')
    parser.add_argument('--system', help="score queries of this system only, e.g. 'duckdb' or 'datafusion' "
                                         '(default: all systems combined)')
    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(-1)
    args = parser.parse_args()
    datasets = get_path_to_dataset(args.results_dir)
    weights = parse_weights(args.weights) if args.weights else None
    w = normalize_weights(weights)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    show = ['compression', 'value_encoding', 'timestamp_encoding', 'max_number_of_rows_in_row_group',
            'statistics_enabled', 'dictionary_enabled'] + [f'{m}_norm' for m in METRICS]
    system_label = args.system or 'all'
    suffix = f'_{args.system}' if args.system else ''
    chosen = []
    for dataset in args.datasets:
        metrics, streams, dropped = compute_metrics(datasets[dataset], args.error_bound, args.min_coverage,
                                                    args.system)
        front = pareto_points(metrics)
        print(f'\n=== {dataset} (query system: {system_label}): {len(front)} Pareto-optimal of {len(metrics)} '
              f'configurations ({len(dropped)} dropped for missing query streams; '
              f'{len(streams)} streams scored: {sorted(set(streams.get_level_values("column_name")))})')
        with pd.option_context('display.width', 250, 'display.max_columns', None, 'display.max_rows', None):
            print(front[show].round(3).to_string(index=False))
        front.to_csv(args.out_dir / f'{dataset}{suffix}_pareto.csv', index=False)
        chosen.append(closest_to_ideal(front, args.top_n, weights).assign(dataset=dataset, system=system_label))

    chosen = pd.concat(chosen, ignore_index=True)
    weights_str = ', '.join(f'{m}={w[m]:.3g}' for m in METRICS)
    print(f'\n=== Chosen configuration per dataset (Pareto-optimal, closest to the ideal point (1, 1, 1); '
          f'weights: {weights_str}; query system: {system_label})')
    with pd.option_context('display.width', 250, 'display.max_columns', None):
        print(chosen[['dataset'] + show + ['ideal_distance']].round(3).to_string(index=False))
    chosen.to_csv(args.out_dir / f'chosen_configurations{suffix}.csv', index=False)
    print(f'\nPareto points per dataset and chosen_configurations{suffix}.csv written to {args.out_dir}/')


if __name__ == '__main__':
    main()
