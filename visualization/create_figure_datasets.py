"""Create the datasets behind the paper's figures from the sqlite databases resulting from the experiments.

Each `*_dataset` function returns the pandas DataFrame that is used to create each figure in the paper, so readers can draw the figures in their own way. The functions read the SQLite databases written by:
1. experiments/scripts/run_native_writers.sh 
2. experiments/scripts/run_low_level_rust_with_default_configs.sh;
3. experiments/runner/test_parameters_in_isolation.py.
 
The script is generally divided into three subsections:
1. Generating datasets for the default configurations of different Parquet implementations.
2. Generating datasets for evaluating individual configurations of Parquet.
3. Generating datasets for evaluating optimized and balanced configurations of Parquet.

1. Generating datasets for the default configurations of different Parquet implementations:

The default writers (C++, Go, Java and Rust, from experiments/scripts/run_native_writers.sh) are compared with
configurations of the low-level Rust writer: its Rust defaults (RustLL) and, per dataset, the configurations with
the lowest storage size, the lowest ingestion time and the lowest DuckDB query time, and the balanced configuration
of compute_pareto.py. optimized_configurations() chooses these, and the figures of this section reuse them.

| Function                               | Output file                                       |
|----------------------------------------|---------------------------------------------------|
| optimized_configurations               | default_writers_optimized_configurations.parquet  |
| native_writer_ingestion_times_dataset  | default_writers_ingestion_times.parquet           |
| low_level_ingestion_times_dataset      | default_writers_low_level_ingestion_times.parquet |
| low_level_storage_sizes_dataset        | default_writers_storage_sizes.parquet             |
| low_level_query_times_dataset          | default_writer_query_times.parquet                |

The last three compare the defaults of C++, Go, Java and Rust, emulated with the low-level Rust writer by
run_low_level_rust_with_default_configs.sh, with the four chosen configurations.

2. Generating datasets for evaluating individual configurations of Parquet:

Individual configurations (one parameter changed at a time, all others at the Rust defaults):

| Function                         | Output file                                           |
|----------------------------------|-------------------------------------------------------|
| row_group_sizes_dataset          | individual_configurations_row_group_sizes.parquet     |
| bloom_filter_dataset             | individual_configurations_bloom_filter.parquet        |
| data_page_sizes_dataset          | individual_configurations_page_sizes.parquet          |
| dictionary_page_sizes_dataset    | individual_configurations_dict_page_sizes.parquet     |
| encodings_dataset                | individual_configurations_encodings.parquet           |
| block_compressors_dataset        | individual_configurations_block_compressors.parquet   |
| statistics_dataset               | individual_configurations_metadata_statistics.parquet |

Every dataset has one row per dataset and evaluated value, with three metrics relative to the dataset's
reference configuration (the Rust defaults, unless noted otherwise):

- size_in_MB: file size;
- compression_time_in_secs: ingestion time;
- execution_time_secs: query time, the geometric mean over all query runs of the configuration by
  query engine (DuckDB or DataFusion).

3. Generating datasets for evaluating optimized and balanced configurations of Parquet:

Combined configurations (experiments/runner/generate_configurations.py --evaluation-type combined_parameters). The
configurations are ranked among those with the pruned parameter values only (_combined_candidates()): bloom filter
and offset index as in the Rust defaults, a 1 MiB data page, a 10 KiB or 1 MiB dictionary page, a row group of
102,400, 1,048,576 or 10,240,000 rows, Brotli 1/4/7, LZ4, uncompressed or Zstd 1/4/7/10/13, and any statistics level.

| Function                              | Output file                                            |
|---------------------------------------|--------------------------------------------------------|
| combined_storage_size_dataset         | combined_configurations_storage_size.parquet           |
| combined_ingestion_time_dataset       | combined_configurations_ingestion_time.parquet         |
| combined_query_time_dataset           | combined_configurations_query_time_{duckdb,datafusion}.parquet |
| combined_balanced_dataset             | combined_configurations_balanced_{duckdb,datafusion}.parquet   |

Each row is one dataset and configuration: output_file_name is the file written with it, configuration its label
(Default for the Rust defaults, TOP-1, TOP-2, ...) and the three metrics are relative to Default.

Usage:
    python create_figure_datasets.py --evaluation-type [default_writers, individual_configurations, combined_configurations]
                                     [--database redd=/path/to/redd.db ...] [--system duckdb] [--output-dir DIR]

--evaluation-type selects the subsection whose datasets are written.
default_writers also reads the database of experiments/scripts/run_native_writers.sh, given with
--native-writers-database (default: <repo>/data/output/native_writers.db).

Without --database, every <repo>/data/output/<dataset>.db that exists is used.
"""
import argparse
import sqlite3
from functools import lru_cache, partial
from pathlib import Path

import numpy as np
import pandas as pd

from compute_pareto import METRICS as PARETO_METRICS, closest_to_ideal, compute_metrics, pareto_points

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RESULTS_DIR = REPO_ROOT / 'data' / 'output'
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / 'figure_datasets'
DEFAULT_NATIVE_WRITERS_DATABASE = DEFAULT_RESULTS_DIR / 'native_writers.db'
DEFAULT_LOW_LEVEL_DEFAULTS_DATABASE = DEFAULT_RESULTS_DIR / 'low_level_rust_default_configs.db'

DATASETS = ['blued', 'greend', 'redd', 'neon_wind']

# Writer properties that identify a configuration (columns of dataset_struct_properties).
CONFIGURATION_COLUMNS = [
    'compression', 'value_encoding', 'timestamp_encoding', 'dictionary_enabled', 'statistics_enabled',
    'max_number_of_rows_in_row_group', 'data_page_size_limit', 'dictionary_page_size_limit',
    'bloom_filter_enabled', 'offset_index_disabled',
]
METRICS = ['size_in_MB', 'execution_time_secs', 'compression_time_in_secs']

# Rust defaults (parquet-rs WriterProperties::default()): the reference configuration of the individual
# configurations. Every individual configuration changes one of these values.
RUST_DEFAULT_CONFIGURATION = {
    'compression': 'UNCOMPRESSED',
    'value_encoding': 'PLAIN',
    'timestamp_encoding': 'PLAIN',
    'dictionary_enabled': 1,
    'statistics_enabled': 'Page',
    'max_number_of_rows_in_row_group': 1_048_576,
    'data_page_size_limit': 1_048_576,
    'dictionary_page_size_limit': 1_048_576,
    'bloom_filter_enabled': 0,
    'offset_index_disabled': 1,
}

# Evaluated row group sizes (in rows) per dataset.
ROW_GROUP_SIZES = {
    'blued': [210_090, 1_048_576, 10_240_000, 102_400_000, 1_024_000_000],
    'greend': [3_376, 10_240, 102_400, 1_048_576, 10_240_000, 110_618_317],
    'redd': [2_536, 10_240, 102_400, 1_048_576, 10_240_000, 83_083_825],
    'neon_wind': [6_055, 10_240, 102_400, 1_048_576, 10_240_000, 198_385_878],
}
PAGE_SIZES = [1024, 10_240, 102_400, 1_048_576]

# Block compressors as stored in dataset_struct_properties.compression, in the order of the figure legend.
BLOCK_COMPRESSORS = [
    'BROTLI(BrotliLevel(1))', 'BROTLI(BrotliLevel(11))', 'GZIP(GzipLevel(1))', 'GZIP(GzipLevel(9))',
    'LZ4', 'SNAPPY', 'UNCOMPRESSED', 'ZSTD(ZstdLevel(1))', 'ZSTD(ZstdLevel(22))',
]
# parquet-rs records LZ4 as LZ4_RAW.
COMPRESSION_LABELS = {'LZ4_RAW': 'LZ4'}

# Encodings: (label, dictionary_enabled, timestamp_encoding, value_encoding); RLE-DICT is the reference.
ENCODINGS = [
    ('RLE-DICT', 1, 'PLAIN', 'PLAIN'),
    ('PLAIN', 0, 'PLAIN', 'PLAIN'),
    ('DELTA_BINARY_PACKED', 0, 'DELTA_BINARY_PACKED', 'PLAIN'),
    ('BYTE-STREAM-SPLIT', 0, 'PLAIN', 'BYTE_STREAM_SPLIT'),
]
# Statistics levels: stored value -> label, in the order of the figure legend; PAGE is the reference.
STATISTICS_LEVELS = {'None': 'NONE', 'Chunk': 'CHUNK', 'Page': 'PAGE'}


def dataset_label(dataset_name):
    """Dataset name as shown in the figures: BLUED, GREEND, REDD and 2D-Wind."""
    return '2D-Wind' if dataset_name == 'neon_wind' else dataset_name.upper()


def row_group_dataset_label(dataset_name):
    """Dataset name in the row group sizes dataset: BLUED, GREEND, Redd and NEON_WIND."""
    return 'Redd' if dataset_name == 'redd' else dataset_name.upper()


def geomean(values):
    return float(np.exp(np.log(np.asarray(values, dtype=float)).mean()))


@lru_cache(maxsize=None)
def load_configurations(database_path, system_name='duckdb'):
    """One row per lossless configuration (error_bound 0) of a result database, with its writer properties and
    raw metrics: size_in_MB and compression_time_in_secs (mean over re-runs) and execution_time_secs (geometric
    mean over the query runs on `system_name`). Identical rows (e.g. a run recorded twice) count once.

    compression_time_in_secs is in whole seconds (as stored in dataset_struct_properties).
    precise_compression_time_in_secs is the same time with its fraction, from file_properties; it is only used to
    break ties between whole seconds.
    """
    connection = sqlite3.connect(f'file:{database_path}?mode=ro', uri=True)
    try:
        ingestion = pd.read_sql_query(
            f"""SELECT DISTINCT output_file_name, {', '.join(CONFIGURATION_COLUMNS)},
                       file_size_in_bytes, compression_time_in_secs
                FROM dataset_struct_properties WHERE error_bound = '0'""", connection)
        queries = pd.read_sql_query(
            """SELECT DISTINCT output_file_name, query_id, column_name, system_name, execution_time_ms, created_at
               FROM query_runs WHERE system_name = ?""", connection, params=(system_name,))
        has_file_properties = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'file_properties'").fetchone()
        precise_times = pd.read_sql_query(
            """SELECT DISTINCT output_file_name, file_absolute_path, compression_time_in_secs
               FROM file_properties WHERE error_bound = '0'""", connection) if has_file_properties else None
    finally:
        connection.close()
    if precise_times is not None and not precise_times.empty:
        precise_times = (precise_times.groupby('output_file_name')['compression_time_in_secs'].mean()
                         .rename('precise_compression_time_in_secs'))
        ingestion = ingestion.join(precise_times, on='output_file_name')
        ingestion['precise_compression_time_in_secs'] = ingestion['precise_compression_time_in_secs'].fillna(
            ingestion['compression_time_in_secs'])
    else:
        ingestion['precise_compression_time_in_secs'] = ingestion['compression_time_in_secs']
    if queries.empty:
        raise ValueError(f"No query runs of '{system_name}' in {database_path}")

    # query_runs.execution_time_ms holds microseconds despite its name.
    queries['execution_time_secs'] = queries['execution_time_ms'] / 1000 ** 2
    queries = queries.merge(ingestion[['output_file_name'] + CONFIGURATION_COLUMNS].drop_duplicates(),
                            on='output_file_name')
    query_time = queries.groupby(CONFIGURATION_COLUMNS).agg(execution_time_secs=('execution_time_secs', geomean))

    ingestion['size_in_MB'] = ingestion['file_size_in_bytes'] / (1000 * 1000)
    ingestion_metrics = ingestion.groupby(CONFIGURATION_COLUMNS).agg(
        size_in_MB=('size_in_MB', 'mean'), compression_time_in_secs=('compression_time_in_secs', 'mean'),
        precise_compression_time_in_secs=('precise_compression_time_in_secs', 'mean'))
    # Only configurations that were also queried.
    return ingestion_metrics.join(query_time, how='inner').reset_index()


def _individual_configurations(configurations, parameter_values):
    """The configurations that equal the Rust defaults except for the given {column: [values]}."""
    selected = configurations
    for column, default in RUST_DEFAULT_CONFIGURATION.items():
        allowed = parameter_values.get(column, [default])
        selected = selected[selected[column].isin(allowed)]
    return selected.copy()


def _relative_to(df, reference_mask, dataset_name):
    """Divides the metrics by those of the reference configuration."""
    reference = df[reference_mask]
    if len(reference) != 1:
        raise ValueError(f'{dataset_name}: expected one reference configuration, found {len(reference)}')
    for metric in METRICS:
        df[metric] = df[metric] / reference[metric].iloc[0]
    return df


def _concat(frames):
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# SUBSECTION
# 1. Default configurations of the Parquet implementations

# Configurations of the low-level Rust writer compared with the default writers, in the order of the figures:
# the Rust defaults (RustLL) and the four configurations chosen per dataset by optimized_configurations().
OPTIMIZED_CONFIGURATIONS = ['RustLL', 'Compression', 'Ingestion time', 'Query time', 'Balanced']
# writer_runs.writer of run_native_writers.sh -> label in the figures. RustHL is default-rust-writer.
NATIVE_WRITERS = {'cpp': 'C++', 'go': 'Go', 'java': 'Java', 'rust': 'RustHL'}
NATIVE_WRITER_INGESTION_CATEGORIES = list(NATIVE_WRITERS.values()) + OPTIMIZED_CONFIGURATIONS

# Configurations the Compression, Ingestion time, Query time and Balanced configurations are chosen from:
# the values kept after pruning (the combined configurations), without Snappy, GZIP and bloom filters.
EXCLUDED_COMPRESSORS = ('SNAPPY', 'GZIP')
CANDIDATE_DATA_PAGE_SIZES = [1_048_576]
CANDIDATE_DICTIONARY_PAGE_SIZES = [10_240, 1_048_576]
CANDIDATE_ROW_GROUP_SIZES = [102_400, 1_048_576, 10_240_000]


def _candidate_configurations(configurations):
    """The configurations optimized_configurations() chooses from (see the constants above)."""
    return configurations[
        ~configurations['compression'].str.upper().str.startswith(EXCLUDED_COMPRESSORS)
        & (configurations['bloom_filter_enabled'] == 0)
        & configurations['data_page_size_limit'].isin(CANDIDATE_DATA_PAGE_SIZES)
        & configurations['dictionary_page_size_limit'].isin(CANDIDATE_DICTIONARY_PAGE_SIZES)
        & configurations['max_number_of_rows_in_row_group'].isin(CANDIDATE_ROW_GROUP_SIZES)
    ]


def _rust_default_configuration(configurations):
    return _individual_configurations(configurations, {})


@lru_cache(maxsize=None)
def _optimized_configurations_of_dataset(database_path, system_name='duckdb', balanced_system=None):
    configurations = load_configurations(str(database_path), system_name)
    candidates = _candidate_configurations(configurations)
    selected = {
        'RustLL': _rust_default_configuration(configurations),
        'Compression': candidates.nsmallest(1, 'size_in_MB'),
        # Ingestion times are stored in whole seconds, so ties are broken with the fractional time.
        'Ingestion time': candidates.sort_values(
            ['compression_time_in_secs', 'precise_compression_time_in_secs']).head(1),
        'Query time': candidates.nsmallest(1, 'execution_time_secs'),
    }
    # Balanced: the Pareto-optimal candidate closest to the ideal point (compute_pareto.py). The metrics are
    # normalized again by the best candidate, so the excluded configurations do not move the ideal point.
    metrics, _, _ = compute_metrics(str(database_path), system=balanced_system)
    metrics = _candidate_configurations(metrics).copy()
    for metric in PARETO_METRICS:
        metrics[f'{metric}_norm'] = metrics[metric] / metrics[metric].min()
    balanced = closest_to_ideal(pareto_points(metrics), 1)[CONFIGURATION_COLUMNS]
    selected['Balanced'] = configurations.merge(balanced, on=CONFIGURATION_COLUMNS)

    frames = []
    for label in OPTIMIZED_CONFIGURATIONS:
        if len(selected[label]) != 1:
            raise ValueError(f'{database_path}: expected one {label} configuration, found {len(selected[label])}')
        frames.append(selected[label].assign(configuration=label))
    return pd.concat(frames, ignore_index=True)


def optimized_configurations(databases, system_name='duckdb', balanced_system=None):
    """The configurations of the low-level Rust writer that the default writers are compared with, per dataset.
    All but RustLL are chosen from _candidate_configurations(): no Snappy, GZIP or bloom filter, a 1 MiB data page,
    a 10 KiB or 1 MiB dictionary page and a row group of 102,400, 1,048,576 or 10,240,000 rows.

    - RustLL: the Rust defaults (RUST_DEFAULT_CONFIGURATION);
    - Compression: the lowest storage size;
    - Ingestion time: the lowest ingestion time;
    - Query time: the lowest query time (geometric mean over the query runs on `system_name`);
    - Balanced: the Pareto-optimal configuration closest to the ideal point, i.e. compute_pareto.py's
      closest_to_ideal(pareto_points(compute_metrics(...))). Its query score uses the queries of
      `balanced_system`; None (the default, as in compute_pareto.py) combines DuckDB and DataFusion.

    Returns one row per dataset and configuration with the writer properties (CONFIGURATION_COLUMNS) and the
    absolute metrics: size_in_MB, compression_time_in_secs (whole seconds) and execution_time_secs.
    dataset_name is the lower-case dataset name.
    """
    frames = [_optimized_configurations_of_dataset(str(path), system_name, balanced_system).assign(dataset_name=name)
              for name, path in databases.items()]
    df = _concat(frames)
    return df[['dataset_name', 'configuration'] + CONFIGURATION_COLUMNS + METRICS]


def load_native_writer_runs(native_writers_database):
    """Mean ingestion time (total_write_time, s) and file size per dataset and writer of run_native_writers.sh."""
    connection = sqlite3.connect(f'file:{native_writers_database}?mode=ro', uri=True)
    try:
        runs = pd.read_sql_query(
            'SELECT dataset_name, writer, total_write_time, file_size_in_bytes FROM writer_runs', connection)
    finally:
        connection.close()
    return runs.groupby(['dataset_name', 'writer'], as_index=False).mean()


def native_writer_ingestion_times_dataset(databases, system_name='duckdb', native_writers_database=None,
                                          balanced_system=None):
    """Ingestion times of the default writers and the optimized configurations, relative to RustLL.

    One row per dataset (dataset_name in lower case) and one column per category, in the order of the figure:
    C++, Go, Java and RustHL (the default writers, from the native_writers_database of run_native_writers.sh),
    then RustLL, Compression, Ingestion time, Query time and Balanced (optimized_configurations()). The values are
    rounded to one decimal as in the figure. A default writer without a run on the dataset is 0, which the figure
    shows as "Did not compress".
    """
    native_writers_database = native_writers_database or DEFAULT_NATIVE_WRITERS_DATABASE
    native = load_native_writer_runs(native_writers_database)
    selected = optimized_configurations(databases, system_name, balanced_system)
    rows = []
    for dataset_name in databases:
        times = {label: 0.0 for label in NATIVE_WRITERS.values()}
        for _, run in native[native['dataset_name'] == dataset_name].iterrows():
            if run['writer'] in NATIVE_WRITERS:
                times[NATIVE_WRITERS[run['writer']]] = run['total_write_time']
        ours = selected[selected['dataset_name'] == dataset_name].set_index('configuration')
        times.update(ours['compression_time_in_secs'].to_dict())
        reference = times['RustLL']
        rows.append({'dataset_name': dataset_name,
                     **{label: round(times[label] / reference, 1) for label in NATIVE_WRITER_INGESTION_CATEGORIES}})
    return pd.DataFrame(rows, columns=['dataset_name'] + NATIVE_WRITER_INGESTION_CATEGORIES)


# Implementations whose defaults run_low_level_rust_with_default_configs.sh emulates -> label in the figures. Its
# output files are named <dataset>-<language>.parquet.
EMULATED_DEFAULTS = {'cpp': 'C++', 'go': 'Go', 'java': 'Java', 'rust': 'Rust'}
# Bars of the low-level ingestion time, storage size and query time datasets; the emulated Rust defaults are the
# reference.
LOW_LEVEL_CATEGORIES = list(EMULATED_DEFAULTS.values()) + OPTIMIZED_CONFIGURATIONS[1:]
LOW_LEVEL_REFERENCE = 'Rust'


def _emulated_defaults_file_names(dataset_name):
    return {f'{dataset_name}-{language}.parquet': label for language, label in EMULATED_DEFAULTS.items()}


def load_emulated_defaults(low_level_defaults_database, dataset_name):
    """Mean file_size_in_bytes and compression_time_in_secs (whole seconds, as in dataset_struct_properties) per
    emulated implementation (C++, Go, Java, Rust) of run_low_level_rust_with_default_configs.sh on the dataset."""
    connection = sqlite3.connect(f'file:{low_level_defaults_database}?mode=ro', uri=True)
    try:
        runs = pd.read_sql_query(
            """SELECT DISTINCT output_file_name, file_size_in_bytes, compression_time_in_secs
               FROM dataset_struct_properties""", connection)
    finally:
        connection.close()
    runs['configuration'] = runs['output_file_name'].map(_emulated_defaults_file_names(dataset_name))
    return (runs.dropna(subset=['configuration'])
            .groupby('configuration')[['file_size_in_bytes', 'compression_time_in_secs']].mean())


def _load_query_runs(database_path, system_name, error_bound_filter):
    connection = sqlite3.connect(f'file:{database_path}?mode=ro', uri=True)
    try:
        queries = pd.read_sql_query(
            f"""SELECT DISTINCT qr.output_file_name, qr.query_id, qr.column_name, qr.system_name,
                       qr.execution_time_ms, qr.created_at
                FROM query_runs qr WHERE qr.system_name = ? AND qr.output_file_name IN
                    (SELECT output_file_name FROM dataset_struct_properties {error_bound_filter})""",
            connection, params=(system_name,))
        properties = pd.read_sql_query(
            f"""SELECT DISTINCT output_file_name, {', '.join(CONFIGURATION_COLUMNS)}
                FROM dataset_struct_properties {error_bound_filter}""", connection)
    finally:
        connection.close()
    # query_runs.execution_time_ms holds microseconds despite its name.
    queries['execution_time_secs'] = queries['execution_time_ms'] / 1000 ** 2
    return queries, properties


def _wide_relative(values_per_dataset, categories, reference, decimals):
    """One row per dataset, one column per category, divided by the reference category and rounded."""
    rows = []
    for dataset_name, values in values_per_dataset.items():
        rows.append({'dataset_name': dataset_name,
                     **{label: round(values.get(label, 0.0) / values[reference], decimals) for label in categories}})
    return pd.DataFrame(rows, columns=['dataset_name'] + categories)


def _low_level_metric(databases, metric, emulated_metric, system_name, low_level_defaults_database, balanced_system):
    low_level_defaults_database = low_level_defaults_database or DEFAULT_LOW_LEVEL_DEFAULTS_DATABASE
    selected = optimized_configurations(databases, system_name, balanced_system)
    values = {}
    for dataset_name in databases:
        emulated = load_emulated_defaults(low_level_defaults_database, dataset_name)
        if LOW_LEVEL_REFERENCE not in emulated.index:
            raise ValueError(f'{low_level_defaults_database}: no {dataset_name}-rust.parquet run')
        ours = selected[selected['dataset_name'] == dataset_name].set_index('configuration')[metric]
        values[dataset_name] = {**emulated[emulated_metric].to_dict(), **ours.to_dict()}
    return values


def low_level_ingestion_times_dataset(databases, system_name='duckdb', low_level_defaults_database=None,
                                      balanced_system=None):
    """Ingestion times of the emulated default writers and the optimized configurations, relative to Rust.

    One row per dataset (dataset_name in lower case) and one column per bar: C++, Go, Java and Rust (their defaults
    emulated with the low-level Rust writer, from the low_level_defaults_database of
    run_low_level_rust_with_default_configs.sh), then Compression, Ingestion time, Query time and Balanced
    (optimized_configurations()). Times are in whole seconds before dividing, the values are rounded to one decimal
    as in the figure, and an implementation without a run on the dataset is 0.
    """
    values = _low_level_metric(databases, 'compression_time_in_secs', 'compression_time_in_secs', system_name,
                               low_level_defaults_database, balanced_system)
    return _wide_relative(values, LOW_LEVEL_CATEGORIES, LOW_LEVEL_REFERENCE, 1)


def low_level_storage_sizes_dataset(databases, system_name='duckdb', low_level_defaults_database=None,
                                    balanced_system=None):
    """File sizes relative to Rust, rounded to two decimals, with the same layout and configurations as
    low_level_ingestion_times_dataset().
    """
    values = _low_level_metric(databases, 'size_in_MB', 'file_size_in_bytes', system_name,
                               low_level_defaults_database, balanced_system)
    # optimized_configurations() reports MB, the emulated defaults bytes.
    for dataset_values in values.values():
        for label in OPTIMIZED_CONFIGURATIONS[1:]:
            dataset_values[label] *= 1000 * 1000
    return _wide_relative(values, LOW_LEVEL_CATEGORIES, LOW_LEVEL_REFERENCE, 2)


def low_level_query_times_dataset(databases, system_name='duckdb', low_level_defaults_database=None,
                                  balanced_system=None):
    """Every query run on the configurations of
    low_level_ingestion_times_dataset(), in seconds (not relative).

    One row per query run (identical rows count once) with output_file_name (the bar: C++, Go, Java, Rust,
    Compression, Ingestion time, Query time or Balanced, as an ordered categorical), execution_time_secs and
    dataset_name in lower case. The C++, Go, Java and Rust runs are those of run_low_level_rust_with_default_configs.sh
    on its own files; the others are the runs of every file written with the chosen configuration.
    """
    low_level_defaults_database = low_level_defaults_database or DEFAULT_LOW_LEVEL_DEFAULTS_DATABASE
    selected = optimized_configurations(databases, system_name, balanced_system)
    emulated_queries, _ = _load_query_runs(low_level_defaults_database, system_name, '')
    frames = []
    for dataset_name, database_path in databases.items():
        emulated = emulated_queries.assign(
            output_file_name=emulated_queries['output_file_name'].map(_emulated_defaults_file_names(dataset_name)))
        frames.append(emulated.dropna(subset=['output_file_name']).assign(dataset_name=dataset_name))

        queries, properties = _load_query_runs(database_path, system_name, "WHERE error_bound = '0'")
        chosen = selected[(selected['dataset_name'] == dataset_name)
                          & selected['configuration'].isin(OPTIMIZED_CONFIGURATIONS[1:])]
        files = properties.merge(chosen[['configuration'] + CONFIGURATION_COLUMNS], on=CONFIGURATION_COLUMNS)
        ours = queries.merge(files[['output_file_name', 'configuration']], on='output_file_name')
        frames.append(ours.drop(columns='output_file_name').rename(columns={'configuration': 'output_file_name'})
                      .assign(dataset_name=dataset_name))
    df = _concat([frame[['output_file_name', 'execution_time_secs', 'dataset_name']] for frame in frames])
    df['output_file_name'] = pd.Categorical(df['output_file_name'], categories=LOW_LEVEL_CATEGORIES)
    return df.sort_values(['output_file_name'], kind='stable').reset_index(drop=True)

# SUBSECTION
# 2. Individual configurations
def row_group_sizes_dataset(databases, system_name='duckdb'):
    """Row group sizes relative to 1,048,576 rows.

    output_file_name holds the row group size (in rows); dataset_name is BLUED, GREEND, Redd or NEON_WIND.
    """
    frames = []
    for dataset_name, database_path in databases.items():
        df = _individual_configurations(load_configurations(str(database_path), system_name),
                                        {'max_number_of_rows_in_row_group': ROW_GROUP_SIZES[dataset_name]})
        df = df.rename(columns={'max_number_of_rows_in_row_group': 'output_file_name'})
        df = _relative_to(df, df['output_file_name'] == RUST_DEFAULT_CONFIGURATION['max_number_of_rows_in_row_group'],
                          dataset_name)
        df['dataset_name'] = row_group_dataset_label(dataset_name)
        frames.append(df.sort_values('output_file_name')[['output_file_name'] + METRICS + ['dataset_name']])
    return _concat(frames)


def bloom_filter_dataset(databases, system_name='duckdb'):
    """Bloom filter on ('True') relative to off ('False')."""
    frames = []
    for dataset_name, database_path in databases.items():
        df = _individual_configurations(load_configurations(str(database_path), system_name),
                                        {'bloom_filter_enabled': [0, 1]})
        df['output_file_name'] = df['bloom_filter_enabled'].map({0: 'False', 1: 'True'})
        df = _relative_to(df, df['output_file_name'] == 'False', dataset_name)
        df['dataset_name'] = dataset_label(dataset_name)
        frames.append(df.sort_values('output_file_name')[['output_file_name'] + METRICS + ['dataset_name']])
    return _concat(frames)


def _page_sizes_dataset(databases, column, system_name):
    frames = []
    for dataset_name, database_path in databases.items():
        df = _individual_configurations(load_configurations(str(database_path), system_name), {column: PAGE_SIZES})
        df = df.rename(columns={column: 'output_file_name'})
        df = _relative_to(df, df['output_file_name'] == RUST_DEFAULT_CONFIGURATION[column], dataset_name)
        df['dataset_name'] = dataset_label(dataset_name)
        frames.append(df.sort_values('output_file_name')[['output_file_name'] + METRICS + ['dataset_name']])
    return _concat(frames)


def data_page_sizes_dataset(databases, system_name='duckdb'):
    """Data page size limits (in bytes) relative to 1 MiB.

    output_file_name holds the data page size limit.
    """
    return _page_sizes_dataset(databases, 'data_page_size_limit', system_name)


def dictionary_page_sizes_dataset(databases, system_name='duckdb'):
    """Dictionary page size limits (in bytes) relative to 1 MiB, with dictionary encoding on.

    output_file_name holds the dictionary page size limit.
    """
    return _page_sizes_dataset(databases, 'dictionary_page_size_limit', system_name)


def encodings_dataset(databases, system_name='duckdb'):
    """PLAIN, DELTA_BINARY_PACKED (timestamps) and BYTE-STREAM-SPLIT (values), without dictionary encoding,
    relative to dictionary encoding (RLE-DICT)."""
    labels = [label for label, *_ in ENCODINGS]
    frames = []
    for dataset_name, database_path in databases.items():
        df = _individual_configurations(load_configurations(str(database_path), system_name), {
            'dictionary_enabled': [0, 1],
            'timestamp_encoding': ['PLAIN', 'DELTA_BINARY_PACKED'],
            'value_encoding': ['PLAIN', 'BYTE_STREAM_SPLIT'],
        })
        label_of = {(d, ts, val): label for label, d, ts, val in ENCODINGS}
        df['output_file_name'] = [label_of.get(key) for key in
                                  zip(df['dictionary_enabled'], df['timestamp_encoding'], df['value_encoding'])]
        df = df.dropna(subset=['output_file_name'])
        df = _relative_to(df, df['output_file_name'] == 'RLE-DICT', dataset_name)
        df['dataset_name'] = dataset_label(dataset_name)
        df['output_file_name'] = pd.Categorical(df['output_file_name'], categories=labels)
        frames.append(df.sort_values('output_file_name')[['output_file_name'] + METRICS + ['dataset_name']])
    merged = _concat(frames)
    merged['output_file_name'] = pd.Categorical(merged['output_file_name'], categories=labels)
    return merged


def block_compressors_dataset(databases, system_name='duckdb'):
    """Block compressors relative to UNCOMPRESSED.

    compression holds the compressor and level as stored by parquet-rs, e.g. ZSTD(ZstdLevel(22)), and LZ4.
    """
    frames = []
    for dataset_name, database_path in databases.items():
        df = load_configurations(str(database_path), system_name).copy()
        df['compression'] = df['compression'].replace(COMPRESSION_LABELS)
        df = _individual_configurations(df, {'compression': BLOCK_COMPRESSORS})
        df = _relative_to(df, df['compression'] == 'UNCOMPRESSED', dataset_name)
        df['dataset_name'] = dataset_label(dataset_name)
        frames.append(df.sort_values('compression')[['compression'] + METRICS + ['dataset_name']])
    merged = _concat(frames)
    merged['compression'] = pd.Categorical(merged['compression'], categories=BLOCK_COMPRESSORS)
    return merged


def statistics_dataset(databases, system_name='duckdb'):
    """Statistics levels NONE and CHUNK relative to PAGE."""
    labels = list(STATISTICS_LEVELS.values())
    frames = []
    for dataset_name, database_path in databases.items():
        df = _individual_configurations(load_configurations(str(database_path), system_name),
                                        {'statistics_enabled': list(STATISTICS_LEVELS)})
        df['output_file_name'] = pd.Categorical(df['statistics_enabled'].map(STATISTICS_LEVELS), categories=labels)
        df = _relative_to(df, df['output_file_name'] == 'PAGE', dataset_name)
        df['dataset_name'] = dataset_label(dataset_name)
        frames.append(df.sort_values('output_file_name')[['output_file_name'] + METRICS + ['dataset_name']])
    merged = _concat(frames)
    merged['output_file_name'] = pd.Categorical(merged['output_file_name'], categories=labels)
    return merged

# SUBSECTION
# 3. Combined configurations

# Pruned parameter values of the combined configurations; only configurations with these values are evaluated.
COMBINED_COMPRESSORS = [
    'BROTLI(BrotliLevel(1))', 'BROTLI(BrotliLevel(4))', 'BROTLI(BrotliLevel(7))', 'LZ4_RAW', 'UNCOMPRESSED',
    'ZSTD(ZstdLevel(1))', 'ZSTD(ZstdLevel(4))', 'ZSTD(ZstdLevel(7))', 'ZSTD(ZstdLevel(10))', 'ZSTD(ZstdLevel(13))',
]
COMBINED_DATA_PAGE_SIZES = [1_048_576]
COMBINED_DICTIONARY_PAGE_SIZES = [10_240, 1_048_576]
COMBINED_ROW_GROUP_SIZES = [102_400, 1_048_576, 10_240_000]
COMBINED_STATISTICS_LEVELS = list(STATISTICS_LEVELS)

# Columns each configuration is evaluted by, ascending. Ingestion times are stored in whole seconds, so ties are
# broken with the fractional time.
RANKINGS = {
    'storage_size': ['size_in_MB'],
    'ingestion_time': ['compression_time_in_secs', 'precise_compression_time_in_secs'],
    'query_time': ['execution_time_secs'],
}
TOP_CONFIGURATIONS = 3
# Balanced figure: (label suffix, ranking, query engine of the ranking). The two best configurations of each.
BALANCED_RANKINGS = [
    ('ingestion', 'ingestion_time', 'duckdb'),
    ('storage', 'storage_size', 'duckdb'),
    ('query_time_datafusion', 'query_time', 'datafusion'),
    ('query_time_duckdb', 'query_time', 'duckdb'),
]
BALANCED_LABELS = ['best', 'second_best']
COMBINED_COLUMNS = ['output_file_name', 'configuration'] + METRICS + ['dataset_name']


def _combined_candidates(configurations):
    """The configurations with the pruned parameter values (see the constants above)."""
    compression = configurations['compression']
    return configurations[
        compression.isin(COMBINED_COMPRESSORS)
        & (configurations['bloom_filter_enabled'] == RUST_DEFAULT_CONFIGURATION['bloom_filter_enabled'])
        & (configurations['offset_index_disabled'] == RUST_DEFAULT_CONFIGURATION['offset_index_disabled'])
        & configurations['data_page_size_limit'].isin(COMBINED_DATA_PAGE_SIZES)
        & configurations['dictionary_page_size_limit'].isin(COMBINED_DICTIONARY_PAGE_SIZES)
        & configurations['max_number_of_rows_in_row_group'].isin(COMBINED_ROW_GROUP_SIZES)
        & configurations['statistics_enabled'].isin(COMBINED_STATISTICS_LEVELS)
    ]


@lru_cache(maxsize=None)
def _output_file_names(database_path):
    """The output_file_name of every lossless configuration. If a configuration was written under several names,
    the first in alphabetical order is used."""
    connection = sqlite3.connect(f'file:{database_path}?mode=ro', uri=True)
    try:
        names = pd.read_sql_query(
            f"""SELECT DISTINCT output_file_name, {', '.join(CONFIGURATION_COLUMNS)}
                FROM dataset_struct_properties WHERE error_bound = '0'""", connection)
    finally:
        connection.close()
    return names.groupby(CONFIGURATION_COLUMNS, as_index=False)['output_file_name'].min()


def _top_configurations(database_path, ranking, system_name, labels):
    """The best candidates by `ranking` (queries of `system_name`), labelled in order: CONFIGURATION_COLUMNS and
    configuration."""
    candidates = _combined_candidates(load_configurations(database_path, system_name))
    top = candidates.sort_values(RANKINGS[ranking], kind='stable').head(len(labels))
    if len(top) < len(labels):
        raise ValueError(f'{database_path}: only {len(top)} candidate configurations for {ranking}')
    return top[CONFIGURATION_COLUMNS].assign(configuration=labels)


def _labelled_dataset(databases, system_name, select):
    """Default (the Rust defaults) and the configurations chosen by select(database_path) per dataset, with their
    metrics (queries of `system_name`) relative to Default."""
    frames = []
    for dataset_name, database_path in databases.items():
        database_path = str(database_path)
        configurations = load_configurations(database_path, system_name)
        chosen = select(database_path).merge(configurations, on=CONFIGURATION_COLUMNS, how='left')
        missing = chosen[chosen[METRICS].isna().any(axis=1)]
        if not missing.empty:
            raise ValueError(f'{database_path}: no {system_name} query runs for {list(missing["configuration"])}')
        df = pd.concat([_rust_default_configuration(configurations).assign(configuration='Default'), chosen],
                       ignore_index=True)
        df = _relative_to(df, df['configuration'] == 'Default', dataset_name)
        df = df.merge(_output_file_names(database_path), on=CONFIGURATION_COLUMNS, how='left')
        df['dataset_name'] = dataset_label(dataset_name)
        frames.append(df[COMBINED_COLUMNS])
    return _concat(frames)


def _top_dataset(databases, ranking, system_name):
    labels = [f'TOP-{rank}' for rank in range(1, TOP_CONFIGURATIONS + 1)]
    return _labelled_dataset(databases, system_name,
                             lambda database_path: _top_configurations(database_path, ranking, system_name, labels))


def combined_storage_size_dataset(databases, system_name='duckdb'):
    """The three combined configurations with the lowest storage size (TOP-1 to TOP-3) and Default."""
    return _top_dataset(databases, 'storage_size', system_name)


def combined_ingestion_time_dataset(databases, system_name='duckdb'):
    """The three combined configurations with the lowest ingestion time (TOP-1 to TOP-3) and Default."""
    return _top_dataset(databases, 'ingestion_time', system_name)


def combined_query_time_dataset(databases, system_name='duckdb'):
    """The three combined configurations with the lowest query time on `system_name` (TOP-1 to TOP-3) and Default;
    execution_time_secs is the query time on `system_name`."""
    return _top_dataset(databases, 'query_time', system_name)


def combined_balanced_dataset(databases, system_name='duckdb'):
    """The two best combined configurations by ingestion time, storage size, DataFusion query time and DuckDB query
    time, and Default. They are labelled best_<ranking> and second_best_<ranking>, e.g. best_ingestion and
    second_best_query_time_duckdb, so a configuration that is among the best for several rankings has a row for
    each. Ingestion time and storage size are ranked among the configurations with DuckDB query runs;
    execution_time_secs is the query time on `system_name`.
    """
    def select(database_path):
        return pd.concat([_top_configurations(database_path, ranking, ranking_system,
                                              [f'{label}_{suffix}' for label in BALANCED_LABELS])
                          for suffix, ranking, ranking_system in BALANCED_RANKINGS], ignore_index=True)
    return _labelled_dataset(databases, system_name, select)


# 3. Combined configurations. Output file name -> function creating the dataset; the query engine of the query
# time datasets is fixed.
COMBINED_CONFIGURATION_DATASETS = {
    'combined_configurations_storage_size': combined_storage_size_dataset,
    'combined_configurations_ingestion_time': combined_ingestion_time_dataset,
    'combined_configurations_query_time_duckdb':
        lambda databases, system_name: combined_query_time_dataset(databases, 'duckdb'),
    'combined_configurations_query_time_datafusion':
        lambda databases, system_name: combined_query_time_dataset(databases, 'datafusion'),
    'combined_configurations_balanced_duckdb':
        lambda databases, system_name: combined_balanced_dataset(databases, 'duckdb'),
    'combined_configurations_balanced_datafusion':
        lambda databases, system_name: combined_balanced_dataset(databases, 'datafusion'),
}


# 2. Individual configurations. Output file name -> function creating the dataset.
INDIVIDUAL_CONFIGURATION_DATASETS = {
    'individual_configurations_row_group_sizes': row_group_sizes_dataset,
    'individual_configurations_bloom_filter': bloom_filter_dataset,
    'individual_configurations_page_sizes': data_page_sizes_dataset,
    'individual_configurations_dict_page_sizes': dictionary_page_sizes_dataset,
    'individual_configurations_encodings': encodings_dataset,
    'individual_configurations_block_compressors': block_compressors_dataset,
    'individual_configurations_metadata_statistics': statistics_dataset,
}

# 1. Default writers. Output file name -> function creating the dataset.
DEFAULT_WRITER_DATASETS = {
    'default_writers_optimized_configurations': optimized_configurations,
    'default_writers_ingestion_times': native_writer_ingestion_times_dataset,
    'default_writers_low_level_ingestion_times': low_level_ingestion_times_dataset,
    'default_writers_storage_sizes': low_level_storage_sizes_dataset,
    'default_writer_query_times': low_level_query_times_dataset,
}

# --evaluation-type -> {output file name: function creating the dataset}. Empty: not implemented yet.
EVALUATION_TYPES = {
    'default_writers': DEFAULT_WRITER_DATASETS,
    'individual_configurations': INDIVIDUAL_CONFIGURATION_DATASETS,
    'combined_configurations': COMBINED_CONFIGURATION_DATASETS,
}


def find_databases(results_dir=DEFAULT_RESULTS_DIR):
    """{dataset: <results_dir>/<dataset>.db} for the datasets whose database exists, in the order of DATASETS."""
    results_dir = Path(results_dir)
    return {name: results_dir / f'{name}.db' for name in DATASETS if (results_dir / f'{name}.db').is_file()}


def parse_databases(values):
    databases = {}
    for value in values:
        dataset_name, separator, path = value.partition('=')
        if not separator or dataset_name not in DATASETS:
            raise argparse.ArgumentTypeError(f"Expected <dataset>=<path> with dataset one of {DATASETS}, got '{value}'")
        if not Path(path).is_file():
            raise argparse.ArgumentTypeError(f'Database not found: {path}')
        databases[dataset_name] = Path(path)
    # Keep the order of DATASETS, so the figures list the datasets in the same order.
    return {name: databases[name] for name in DATASETS if name in databases}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--evaluation-type', required=True, choices=list(EVALUATION_TYPES),
                        help='subsection whose figure datasets are written')
    parser.add_argument('--database', nargs='+', metavar='DATASET=PATH',
                        help=f'result database per dataset (default: every {DEFAULT_RESULTS_DIR}/<dataset>.db)')
    parser.add_argument('--system', default='duckdb', help="query engine of the query times (default: duckdb)")
    parser.add_argument('--native-writers-database', type=Path, default=DEFAULT_NATIVE_WRITERS_DATABASE,
                        help='database of experiments/scripts/run_native_writers.sh, for --evaluation-type '
                             f'default_writers (default: {DEFAULT_NATIVE_WRITERS_DATABASE})')
    parser.add_argument('--low-level-defaults-database', type=Path, default=DEFAULT_LOW_LEVEL_DEFAULTS_DATABASE,
                        help='database of experiments/scripts/run_low_level_rust_with_default_configs.sh, for '
                             f'--evaluation-type default_writers (default: {DEFAULT_LOW_LEVEL_DEFAULTS_DATABASE})')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    dataset_functions = EVALUATION_TYPES[args.evaluation_type]
    if not dataset_functions:
        parser.error(f"--evaluation-type {args.evaluation_type} is not implemented yet")
    databases = parse_databases(args.database) if args.database else find_databases()
    if not databases:
        parser.error(f'No result databases found in {DEFAULT_RESULTS_DIR}; pass them with --database')
    if args.evaluation_type == 'default_writers':
        if not args.native_writers_database.is_file():
            parser.error(f'Native writers database not found: {args.native_writers_database}. '
                         'Run experiments/scripts/run_native_writers.sh or pass --native-writers-database')
        if not args.low_level_defaults_database.is_file():
            parser.error(f'Database not found: {args.low_level_defaults_database}. Run '
                         'experiments/scripts/run_low_level_rust_with_default_configs.sh or pass '
                         '--low-level-defaults-database')
        dataset_functions = dict(dataset_functions)
        dataset_functions['default_writers_ingestion_times'] = partial(
            native_writer_ingestion_times_dataset, native_writers_database=args.native_writers_database)
        for file_name in ['default_writers_low_level_ingestion_times', 'default_writers_storage_sizes',
                          'default_writer_query_times']:
            dataset_functions[file_name] = partial(
                dataset_functions[file_name], low_level_defaults_database=args.low_level_defaults_database)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    print('Databases: ' + ', '.join(f'{name}={path}' for name, path in databases.items()))
    for file_name, create_dataset in dataset_functions.items():
        df = create_dataset(databases, args.system)
        path = args.output_dir / f'{file_name}.parquet'
        df.to_parquet(path, index=False)
        print(f'{path}: {len(df)} rows')


if __name__ == '__main__':
    main()
