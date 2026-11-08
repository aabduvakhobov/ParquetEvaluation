"""Generate the configurations.yaml read by test_parameters_in_isolation.py.

Evaluation types:
- individual_parameters: every Parquet parameter is varied on its own, all others stay at the BASELINE below.
  The configurations are written as `configurations.manual`, so run them with
  `python test_parameters_in_isolation.py MANUAL`.
- combined_parameters: the product of the chosen parameter values (not implemented yet).

Each configuration lists only the fields that differ from the runner's defaults (ParquetWriterProperties in
test_parameters_in_isolation.py). The baseline turns on dictionary encoding (RLE_DICTIONARY) and PAGE
statistics; a parameter group only changes these two settings when it evaluates them itself.
Configurations that equal one already emitted (e.g. the baseline in every group) are written once and the
duplicates are listed as comments, so no configuration is run twice.

Usage:
    python generate_configurations.py --dataset-name redd --evaluation-type individual_parameters \\
        [--path-to-table-files /data/redd/] [--path-to-save /data/save_data/] \\
        [--path-to-metadata-database redd.db] [--output configurations.yaml]

Dataset and save paths default to <DATASET>_PATH and SAVE_DIR from the environment or from
experiments/scripts/paths.env.
"""
import argparse
import os
import sys
from dataclasses import asdict, replace
from pathlib import Path

import yaml

from test_parameters_in_isolation import ParquetWriterProperties

RUNNER_DIR = Path(__file__).resolve().parent
REPO_ROOT = RUNNER_DIR.parent.parent
PATHS_ENV = REPO_ROOT / 'experiments' / 'scripts' / 'paths.env'
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
            (f'{c}({lvl})' if lvl else c, replace(b, compression=c, compression_level=lvl))
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


def _effective_key(config: ParquetWriterProperties) -> tuple:
    """Configurations that produce the same file compare equal.

    parquet-rs writes the offset index anyway when PAGE statistics are on, so the flag is ignored then.
    """
    d = asdict(config)
    if d['statistics_enabled'] == 'PAGE':
        d['offset_index_disabled'] = None
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


def render_individual_configurations(dataset_name: str) -> tuple[str, int]:
    lines = ['  manual:']
    seen = {}
    count = 0

    def emit(group, label, config):
        nonlocal count
        key = _effective_key(config)
        if key in seen:
            lines.append(f'    # {group}={label}: same configuration as {seen[key]}')
            return
        seen[key] = f'{group}={label}'
        lines.append(f'    # {group}={label}')
        lines.append(f'    - {_flow(config)}')
        count += 1

    lines.append('    # ---- baseline: reference point of every parameter group ----')
    emit('baseline', 'dictionary+page_statistics', BASELINE)
    for group, configs in individual_parameter_groups(dataset_name).items():
        lines.append(f'    # ---- {group} ----')
        for label, config in configs:
            emit(group, label, config)
    return '\n'.join(lines) + '\n', count


def _load_paths_env() -> dict:
    values = {}
    if PATHS_ENV.is_file():
        for line in PATHS_ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                values[key.strip()] = value.strip().strip('"\'')
    return values


def _setting(name: str, paths_env: dict):
    return os.environ.get(name) or paths_env.get(name)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dataset-name', required=True, choices=list(ROW_GROUP_SIZES))
    parser.add_argument('--evaluation-type', required=True, choices=EVALUATION_TYPES)
    parser.add_argument('--path-to-table-files', help='dataset directory or file (default: <DATASET>_PATH)')
    parser.add_argument('--path-to-save', help='directory for the written Parquet files (default: SAVE_DIR)')
    parser.add_argument('--path-to-metadata-database', help='SQLite database for the results (default: <dataset>.db)')
    parser.add_argument('--path-to-executable', default=str(DEFAULT_EXECUTABLE))
    parser.add_argument('--query-sample-size-per-interval', type=int, default=100)
    parser.add_argument('--output', type=Path, default=Path('configurations.yaml'))
    args = parser.parse_args()

    if args.evaluation_type == 'combined_parameters':
        sys.exit('combined_parameters is not implemented yet')

    paths_env = _load_paths_env()
    table_files = args.path_to_table_files or _setting(f'{args.dataset_name.upper()}_PATH', paths_env)
    save_dir = args.path_to_save or _setting('SAVE_DIR', paths_env)
    missing = [name for name, value in [('--path-to-table-files', table_files), ('--path-to-save', save_dir)]
               if not value]
    if missing:
        sys.exit(f"Missing {', '.join(missing)}: pass them or set them in {PATHS_ENV} / the environment")

    setup = {
        'path_to_executable': args.path_to_executable,
        'table_name': args.dataset_name,
        'error_bound': [0],
        'path_to_table_files': table_files,
        'path_to_save': save_dir,
        'path_to_metadata_database': args.path_to_metadata_database or f'{args.dataset_name}.db',
        'query_sample_size_per_interval': args.query_sample_size_per_interval,
    }
    configurations, count = render_individual_configurations(args.dataset_name)

    header = (f'# Generated by generate_configurations.py --dataset-name {args.dataset_name} '
              f'--evaluation-type {args.evaluation_type}\n'
              f'# {count} configurations. Run with: python test_parameters_in_isolation.py MANUAL\n')
    text = (header + yaml.safe_dump({'experimental_setup': setup}, sort_keys=False)
            + '\nconfigurations:\n' + configurations)
    yaml.safe_load(text)  # fail early if the output is not valid YAML
    args.output.write_text(text)
    print(f'Wrote {count} configurations to {args.output}')


if __name__ == '__main__':
    main()
