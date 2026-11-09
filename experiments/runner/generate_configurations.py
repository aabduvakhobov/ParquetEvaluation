"""Generate the configurations.yaml read by test_parameters_in_isolation.py.

Evaluation types:
- individual_parameters: every Parquet parameter is varied on its own, all others stay at the BASELINE below.
  The configurations are written as `configurations.manual`, so run them with
  `python test_parameters_in_isolation.py MANUAL`.
- combined_parameters: the product of the parameter values kept after pruning (COMBINED_* below), with the
  bloom filter and offset index at the runner's defaults, plus the hand-picked COMBINED_EXTRA_CONFIGURATIONS for
  Brotli 11 and Zstd 22. Also written as `configurations.manual`.

Each configuration lists only the fields that differ from the runner's defaults (ParquetWriterProperties in
test_parameters_in_isolation.py). The baseline turns on dictionary encoding (RLE_DICTIONARY) and PAGE
statistics; a parameter group only changes these two settings when it evaluates them itself.
Configurations that equal one already emitted (e.g. the baseline in every group) are written once and the
duplicates are listed as comments, so no configuration is run twice.

Usage:
    python generate_configurations.py --dataset-name redd --evaluation-type individual_parameters \\
        [--path-to-save /path/to/save_data/] [--path-to-metadata-database redd.db] [--output configurations.yaml]

The dataset is read from <repo>/data/<dataset>/ (e.g. <repo>/data/redd/ for redd), like
experiments/scripts/run_native_writers.sh and run_low_level_rust_with_default_configs.sh.
The written Parquet files go to --path-to-save, by default <repo>/experiments/writers/outputs/save_data/.
"""
import argparse
import itertools
import sys
from dataclasses import asdict, replace
from pathlib import Path

import yaml

from test_parameters_in_isolation import ParquetWriterProperties

RUNNER_DIR = Path(__file__).resolve().parent
REPO_ROOT = RUNNER_DIR.parent.parent
DATA_DIR = REPO_ROOT / 'data'
DEFAULT_SAVE_DIR = REPO_ROOT / 'experiments' / 'writers' / 'outputs' / 'save_data'
DEFAULT_EXECUTABLE = REPO_ROOT / 'experiments' / 'writers' / 'rust' / 'target' / 'release' / 'low-level-writer'

EVALUATION_TYPES = ['individual_parameters', 'combined_parameters']

# Evaluated row group sizes (in rows) per dataset; the last one is the whole dataset in one row group.
ROW_GROUP_SIZES = {
    'blued': [210_090, 1_048_576, 10_240_000, 102_400_000, 1_024_000_000, 6_884_208_476],
    'greend': [3_376, 10_240, 102_400, 1_048_576, 10_240_000, 110_618_317],
    'redd': [2_536, 10_240, 102_400, 1_048_576, 10_240_000, 83_083_825],
    'neon_wind': [6_055, 10_240, 102_400, 1_048_576, 10_240_000, 198_385_878],
}

# Defaults of the runner (ParquetWriterProperties in test_parameters_in_isolation.py). The YAML only lists the
# fields that differ from these; the runner fills in the rest.
DEFAULTS = ParquetWriterProperties()

# Baseline: the defaults with dictionary encoding and PAGE statistics turned on. The default
# offset_index_disabled=True has no effect here, since parquet-rs always writes the offset index while PAGE
# statistics are enabled.
BASELINE = replace(DEFAULTS, dictionary_enabled=True, statistics_enabled='PAGE')

# Statistics level used when the offset index is disabled: parquet-rs ignores `offset_index_disabled`
# while PAGE statistics are on, so page statistics must be turned off for this configuration.
STATISTICS_WITHOUT_OFFSET_INDEX = 'CHUNK'

# (compression, level); level 0 for compressors without levels.
BLOCK_COMPRESSORS = [
    ('GZIP', 1), ('GZIP', 9),
    ('BROTLI', 1), ('BROTLI', 11),
    ('ZSTD', 1), ('ZSTD', 22),
    ('SNAPPY', 0), ('LZ4', 0), ('UNCOMPRESSED', 0),
]
PAGE_SIZES = [1024, 10240, 102400, 1048576]

# RLE_DICTIONARY cannot be set as a column encoding in parquet-rs: it is used through `dictionary_enabled`
# (which applies to all columns), with the column encoding as the fallback. Every other encoding is
# therefore evaluated with dictionary encoding off.
VALUE_ENCODINGS = ['BYTE_STREAM_SPLIT', 'RLE_DICTIONARY', 'PLAIN']
TIMESTAMP_ENCODINGS = ['DELTA_BINARY_PACKED', 'RLE_DICTIONARY', 'PLAIN']
STATISTICS_LEVELS = ['PAGE', 'CHUNK', 'NONE']

# combined_parameters: the values kept after pruning (table "Maintained parameters after pruning" in the paper).
# The same values are used for every dataset. Bloom filter and offset index are not combined and stay at the
# runner's defaults (no bloom filter; the offset index is only written with PAGE statistics).
COMBINED_ROW_GROUP_SIZES = [102_400, 1_048_576, 10_240_000]
COMBINED_DATA_PAGE_SIZES = [1_048_576]
COMBINED_DICTIONARY_PAGE_SIZES = [10_240, 1_048_576]
COMBINED_BLOCK_COMPRESSORS = [
    ('BROTLI', 1), ('BROTLI', 4), ('BROTLI', 7),
    ('LZ4', 0),
    ('UNCOMPRESSED', 0),
    ('ZSTD', 1), ('ZSTD', 4), ('ZSTD', 7), ('ZSTD', 10), ('ZSTD', 13),
]
COMBINED_VALUE_ENCODINGS = VALUE_ENCODINGS
COMBINED_TIMESTAMP_ENCODINGS = TIMESTAMP_ENCODINGS
COMBINED_STATISTICS_LEVELS = STATISTICS_LEVELS

# combined_parameters: hand-picked configurations added for the highest block compressor levels, which are not part
# of the product above. Every configuration is run with each of these compressors; fields that are not listed
# keep the runner's defaults (row group 1,048,576 rows, data page 1 MiB, no dictionary, no statistics).
COMBINED_EXTRA_COMPRESSORS = [('BROTLI', 11), ('ZSTD', 22)]
COMBINED_EXTRA_CONFIGURATIONS = [
    dict(dictionary_enabled=True, dictionary_page_size_limit=10_240),
    dict(dictionary_enabled=True, dictionary_page_size_limit=10_240, statistics_enabled='PAGE'),
    dict(dictionary_enabled=True, dictionary_page_size_limit=10_240,
         timestamp_encoding='DELTA_BINARY_PACKED'),
    dict(value_encoding='BYTE_STREAM_SPLIT', timestamp_encoding='DELTA_BINARY_PACKED'),
    dict(timestamp_encoding='DELTA_BINARY_PACKED'),
    dict(dictionary_enabled=True),
    dict(value_encoding='BYTE_STREAM_SPLIT', timestamp_encoding='DELTA_BINARY_PACKED', statistics_enabled='PAGE'),
]

# Fallback encodings that may be combined with dictionary encoding: Delta-Binary-Packed can, Byte-Stream-Split cannot.
ENCODINGS_INCOMPATIBLE_WITH_DICTIONARY = {'BYTE_STREAM_SPLIT'}


def _encoding(base: ParquetWriterProperties, column: str, encoding: str) -> ParquetWriterProperties:
    if encoding == 'RLE_DICTIONARY':
        return replace(base, dictionary_enabled=True)
    return replace(base, dictionary_enabled=False, **{column: encoding})


def _offset_index(base: ParquetWriterProperties, enabled: bool) -> ParquetWriterProperties:
    if enabled:
        return replace(base, offset_index_disabled=False, statistics_enabled='PAGE')
    return replace(base, offset_index_disabled=True, statistics_enabled=STATISTICS_WITHOUT_OFFSET_INDEX)


def individual_parameter_groups(dataset_name: str) -> dict:
    """Parameter group -> list of (label, configuration)."""
    b = BASELINE
    return {
        'block_compressor': [
            (_compressor_label(c, lvl), replace(b, compression=c, compression_level=lvl))
            for c, lvl in BLOCK_COMPRESSORS],
        'data_page_size_limit': [(str(s), replace(b, data_page_size_limit=s)) for s in PAGE_SIZES],
        'dictionary_page_size_limit': [(str(s), replace(b, dictionary_page_size_limit=s)) for s in PAGE_SIZES],
        'value_encoding': [(e, _encoding(b, 'value_encoding', e)) for e in VALUE_ENCODINGS],
        'timestamp_encoding': [(e, _encoding(b, 'timestamp_encoding', e)) for e in TIMESTAMP_ENCODINGS],
        'bloom_filter_enabled': [(str(v).lower(), replace(b, bloom_filter_enabled=v)) for v in (True, False)],
        'offset_index_enabled': [(str(v).lower(), _offset_index(b, v)) for v in (True, False)],
        'statistics_enabled': [(s, replace(b, statistics_enabled=s)) for s in STATISTICS_LEVELS],
        'max_row_group_size': [(str(s), replace(b, max_row_group_size=s)) for s in ROW_GROUP_SIZES[dataset_name]],
    }


def _compressor_label(compression: str, level: int) -> str:
    return f'{compression}({level})' if level else compression


def _combined_encodings(base: ParquetWriterProperties, value_encoding: str, timestamp_encoding: str):
    """Applies a (value, timestamp) encoding pair; None if the pair is not evaluated.

    `dictionary_enabled` switches dictionary encoding on for all columns, and each column's encoding is then the
    fallback used once its dictionary page is full. A pair with RLE_DICTIONARY therefore turns the dictionary on,
    and the other column's encoding becomes its fallback (PLAIN for an RLE_DICTIONARY column). Dictionary
    encoding is combined with DELTA_BINARY_PACKED, but not with BYTE_STREAM_SPLIT.
    """
    if 'RLE_DICTIONARY' not in (value_encoding, timestamp_encoding):
        return replace(base, dictionary_enabled=False, value_encoding=value_encoding,
                       timestamp_encoding=timestamp_encoding)
    value_fallback = DEFAULTS.value_encoding if value_encoding == 'RLE_DICTIONARY' else value_encoding
    timestamp_fallback = DEFAULTS.timestamp_encoding if timestamp_encoding == 'RLE_DICTIONARY' else timestamp_encoding
    if {value_fallback, timestamp_fallback} & ENCODINGS_INCOMPATIBLE_WITH_DICTIONARY:
        return None
    return replace(base, dictionary_enabled=True, value_encoding=value_fallback,
                   timestamp_encoding=timestamp_fallback)


def combined_configurations():
    """(label, configuration) for every evaluated combination of the COMBINED_* values."""
    for row_group_size, data_page_size, (compression, level), statistics, value_encoding, timestamp_encoding \
            in itertools.product(COMBINED_ROW_GROUP_SIZES, COMBINED_DATA_PAGE_SIZES, COMBINED_BLOCK_COMPRESSORS,
                                 COMBINED_STATISTICS_LEVELS, COMBINED_VALUE_ENCODINGS, COMBINED_TIMESTAMP_ENCODINGS):
        base = replace(DEFAULTS, max_row_group_size=row_group_size, data_page_size_limit=data_page_size,
                       compression=compression, compression_level=level, statistics_enabled=statistics)
        config = _combined_encodings(base, value_encoding, timestamp_encoding)
        if config is None:
            continue
        label = (f'row_group={row_group_size} data_page={data_page_size} '
                 f'compressor={_compressor_label(compression, level)} statistics={statistics} '
                 f'value={value_encoding} timestamp={timestamp_encoding}')
        if config.dictionary_enabled:
            # The dictionary page size only matters when dictionary encoding is on.
            for dictionary_page_size in COMBINED_DICTIONARY_PAGE_SIZES:
                yield (f'{label} dictionary_page={dictionary_page_size}',
                       replace(config, dictionary_page_size_limit=dictionary_page_size))
        else:
            yield label, config


def combined_extra_configurations():
    """(label, configuration) for COMBINED_EXTRA_CONFIGURATIONS with each of COMBINED_EXTRA_COMPRESSORS."""
    for compression, level in COMBINED_EXTRA_COMPRESSORS:
        for fields in COMBINED_EXTRA_CONFIGURATIONS:
            config = replace(DEFAULTS, compression=compression, compression_level=level, **fields)
            if config.dictionary_enabled and {config.value_encoding, config.timestamp_encoding} \
                    & ENCODINGS_INCOMPATIBLE_WITH_DICTIONARY:
                raise ValueError(f'dictionary encoding cannot be combined with {fields}')
            settings = ' '.join(f'{k}={v}' for k, v in fields.items())
            yield f'compressor={_compressor_label(compression, level)} {settings}', config


def _effective_key(config: ParquetWriterProperties) -> tuple:
    """Configurations that produce the same file compare equal.

    parquet-rs writes the offset index anyway when PAGE statistics are on, so the flag is ignored then.
    The dictionary page size has no effect while dictionary encoding is off.
    """
    d = asdict(config)
    if d['statistics_enabled'] == 'PAGE':
        d['offset_index_disabled'] = None
    if not d['dictionary_enabled']:
        d['dictionary_page_size_limit'] = None
    return tuple(sorted((k, str(v)) for k, v in d.items()))


def _non_default_fields(config: ParquetWriterProperties) -> dict:
    """Only the fields that differ from the runner's defaults."""
    defaults = asdict(DEFAULTS)
    changed = {k: v for k, v in asdict(config).items() if v != defaults[k]}
    # The evaluated parameter first, then the baseline's dictionary/statistics settings.
    baseline_keys = ('dictionary_enabled', 'statistics_enabled')
    return {**{k: v for k, v in changed.items() if k not in baseline_keys},
            **{k: changed[k] for k in baseline_keys if k in changed}}


def _flow(config: ParquetWriterProperties) -> str:
    return yaml.safe_dump(_non_default_fields(config), default_flow_style=True, sort_keys=False,
                          width=10_000).strip()


class _ManualConfigurations:
    """Collects the `configurations.manual` entries, writing each distinct configuration once."""

    def __init__(self, list_duplicates: bool = True):
        self.lines = ['  manual:']
        self.seen = {}
        self.count = 0
        self.duplicates = 0
        self.list_duplicates = list_duplicates

    def section(self, title: str):
        self.lines.append(f'    # ---- {title} ----')

    def emit(self, label: str, config: ParquetWriterProperties):
        key = _effective_key(config)
        if key in self.seen:
            self.duplicates += 1
            if self.list_duplicates:
                self.lines.append(f'    # {label}: same configuration as {self.seen[key]}')
            return
        self.seen[key] = label
        self.lines.append(f'    # {label}')
        self.lines.append(f'    - {_flow(config)}')
        self.count += 1

    def render(self) -> tuple[str, int, int]:
        return '\n'.join(self.lines) + '\n', self.count, self.duplicates


def render_individual_configurations(dataset_name: str) -> tuple[str, int, int]:
    manual = _ManualConfigurations()
    manual.section('baseline: reference point of every parameter group')
    manual.emit('baseline=dictionary+page_statistics', BASELINE)
    for group, configs in individual_parameter_groups(dataset_name).items():
        manual.section(group)
        for label, config in configs:
            manual.emit(f'{group}={label}', config)
    return manual.render()


def render_combined_configurations() -> tuple[str, int, int]:
    # Pairs such as (RLE_DICTIONARY, PLAIN) and (RLE_DICTIONARY, RLE_DICTIONARY) give the same configuration; with
    # hundreds of them, they are only counted in the header instead of being listed.
    manual = _ManualConfigurations(list_duplicates=False)
    current_row_group_size = None
    for label, config in combined_configurations():
        if config.max_row_group_size != current_row_group_size:
            current_row_group_size = config.max_row_group_size
            manual.section(f'max_row_group_size={current_row_group_size}')
        manual.emit(label, config)
    manual.section('hand-picked configurations for BROTLI(11) and ZSTD(22)')
    for label, config in combined_extra_configurations():
        manual.emit(label, config)
    return manual.render()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dataset-name', required=True, choices=list(ROW_GROUP_SIZES))
    parser.add_argument('--evaluation-type', required=True, choices=EVALUATION_TYPES)
    parser.add_argument('--path-to-save', help='directory for the written Parquet files '
                        '(default: <repo>/experiments/writers/outputs/save_data/)')
    parser.add_argument('--path-to-metadata-database', help='SQLite database for the results (default: <dataset>.db)')
    parser.add_argument('--path-to-executable', default=str(DEFAULT_EXECUTABLE))
    parser.add_argument('--query-sample-size-per-interval', type=int, default=100)
    parser.add_argument('--output', type=Path, default=Path('configurations.yaml'))
    args = parser.parse_args()

    dataset_dir = DATA_DIR / args.dataset_name
    if not any(dataset_dir.glob('*.parquet')):
        sys.exit(f"No Parquet files found in {dataset_dir}. Download the {args.dataset_name} dataset there first.")
    table_files = str(dataset_dir)
    save_dir = args.path_to_save or str(DEFAULT_SAVE_DIR)
    Path(save_dir).mkdir(parents=True, exist_ok=True)

    setup = {
        'path_to_executable': args.path_to_executable,
        'table_name': args.dataset_name,
        'error_bound': [0],
        'path_to_table_files': table_files,
        'path_to_save': save_dir,
        'path_to_metadata_database': args.path_to_metadata_database or f'{args.dataset_name}.db',
        'query_sample_size_per_interval': args.query_sample_size_per_interval,
    }
    if args.evaluation_type == 'individual_parameters':
        configurations, count, duplicates = render_individual_configurations(args.dataset_name)
    else:
        configurations, count, duplicates = render_combined_configurations()

    header = (f'# Generated by generate_configurations.py --dataset-name {args.dataset_name} '
              f'--evaluation-type {args.evaluation_type}\n'
              f'# {count} configurations ({duplicates} combinations that equal one of them are left out). '
              f'Run with: python test_parameters_in_isolation.py MANUAL\n')
    text = (header + yaml.safe_dump({'experimental_setup': setup}, sort_keys=False)
            + '\nconfigurations:\n' + configurations)
    yaml.safe_load(text)  # fail early if the output is not valid YAML
    args.output.write_text(text)
    print(f'Wrote {count} configurations to {args.output}')


if __name__ == '__main__':
    main()
