# Visualization

Generates the dataset that used for figures in the paper from the results of the experiments.

| File | Purpose |
|---|---|
| [create_figure_datasets.py](create_figure_datasets.py) | Builds the dataset behind each figure from the result databases, so you can draw the figures yourself.
| [compute_pareto.py](compute_pareto.py) | Pareto-optimal configurations for storage size, ingestion time and query time. Also picks the *balanced* configuration. |

## Workflow

1. **Run the experiments** described in [experiments/README.md](../experiments/README.md). They write their SQLite databases to `<repo>/data/output/`:
   - `<dataset>.db` from `test_parameters_in_isolation.py`, one per dataset;
   - `native_writers.db` from `run_native_writers.sh`;
   - `low_level_rust_default_configs.db` from `run_low_level_rust_with_default_configs.sh`.
2. **Create the figure datasets** with `create_figure_datasets.py`, once per evaluation type (see below). It writes them to `figure_datasets/`.
3. **Draw the figures** by passing each dataset to its plotting function in the notebook.

```bash
pip install -r ../requirements.txt
# For Figure datasets in Section IV
python create_figure_datasets.py --evaluation-type default_writers
# For Figure datasets in Section V
python create_figure_datasets.py --evaluation-type individual_configurations
# For Figure datasets in Section VI
python create_figure_datasets.py --evaluation-type combined_configurations
```

[compute_pareto.py](compute_pareto.py) reads the same `<repo>/data/output/<dataset>.db` databases, or those in `--results-dir`:

```bash
python compute_pareto.py --datasets blued redd [--results-dir path/to/dir] [--system duckdb]
```

## Figure datasets

[create_figure_datasets.py](create_figure_datasets.py) has one function per figure. Each returns the pandas DataFrame with the data behind that figure.

### Default writers

The default writers are compared with five configurations of the low-level Rust writer per dataset, chosen by `optimized_configurations`:
- **RustLL:** the Rust defaults.
- **Low Storage Size:** the lowest storage size.
- **Low Ingestion time:** the lowest ingestion time. Ingestion times are stored in whole seconds, so ties are broken with the fractional time in `file_properties`.
- **Low Query time:** the lowest DuckDB query time, as the geometric mean over its query runs.
- **Balanced:** the configuration chosen by `closest_to_ideal()` in [compute_pareto.py](compute_pareto.py), with its default settings.

All but RustLL are chosen only from configurations without Snappy, GZIP or a bloom filter, with a 1 MiB data page, a 10 KiB or 1 MiB dictionary page and a row group of 102,400, 1,048,576 or 10,240,000 rows.

| Function | Output file |
|---|---|
| `optimized_configurations` | `default_writers_optimized_configurations.parquet` |
| `native_writer_ingestion_times_dataset` | `default_writers_ingestion_times.parquet` |
| `low_level_ingestion_times_dataset` | `default_writers_low_level_ingestion_times.parquet` |
| `low_level_storage_sizes_dataset` | `default_writers_storage_sizes.parquet` |
| `low_level_query_times_dataset` | `default_writer_query_times.parquet` |

`native_writer_ingestion_times_dataset` also reads the database of [run_native_writers.sh](../experiments/scripts/run_native_writers.sh), `data/output/native_writers.db` by default or `--native-writers-database`. It has one row per dataset and one column per bar, relative to RustLL and rounded to one decimal. A writer without a run on a dataset is 0, which the figure shows as "Did not compress".

The last three compare the C++, Go, Java and Rust defaults, emulated with the low-level Rust writer, with Low Ingestion Time, Low Storage Size and Low Query time and Balanced. The emulated defaults come from the database of [run_low_level_rust_with_default_configs.sh](../experiments/scripts/run_low_level_rust_with_default_configs.sh), `data/output/low_level_rust_default_configs.db` by default or `--low-level-defaults-database`.
- **Ingestion times and storage sizes:** one row per dataset and one column per bar, relative to the emulated Rust defaults. Ingestion times are rounded to one decimal and sizes to two, as in the figures.
- **Query times:** one row per DuckDB query run, in seconds, with the bar in `output_file_name`.

### Individual configurations

| Function | Output file |
|---|---|
| `row_group_sizes_dataset` | `individual_configurations_row_group_sizes.parquet` |
| `bloom_filter_dataset` | `individual_configurations_bloom_filter.parquet` |
| `offset_index_dataset` | `individual_configurations_offset_index.parquet` |
| `data_page_sizes_dataset` | `individual_configurations_page_sizes.parquet` |
| `dictionary_page_sizes_dataset` | `individual_configurations_dict_page_sizes.parquet` |
| `encodings_dataset` | `individual_configurations_encodings.parquet` |
| `block_compressors_dataset` | `individual_configurations_block_compressors.parquet` |
| `statistics_dataset` | `individual_configurations_metadata_statistics.parquet` |

Each row is one dataset and one value of the evaluated parameter, with three metrics. Each metric is relative to the configuration with the Rust defaults; for the bloom filter, that's the one without a bloom filter. The offset index is evaluated without statistics, enabled (`True`) relative to disabled (`False`), because parquet-rs always writes the offset index with PAGE statistics.
- `size_in_MB`: file size.
- `compression_time_in_secs`: ingestion time.
- `execution_time_secs`: query time, as the geometric mean over the configuration's query runs.

### Combined configurations

The combined configurations of `generate_configurations.py --evaluation-type combined_parameters` are ranked by each metric. Only configurations with the pruned parameter values take part:
- bloom filter and offset index as in the Rust defaults;
- a 1 MiB data page and a 10 KiB or 1 MiB dictionary page;
- a row group of 102,400, 1,048,576 or 10,240,000 rows;
- Brotli 1/4/7/11, LZ4, uncompressed or Zstd 1/4/7/10/13/22;
- any statistics level: none, column chunk or page.

| Function | Output file |
|---|---|
| `combined_storage_size_dataset` | `combined_configurations_storage_size.parquet` |
| `combined_ingestion_time_dataset` | `combined_configurations_ingestion_time.parquet` |
| `combined_query_time_dataset` | `combined_configurations_query_time_duckdb.parquet` and `combined_configurations_query_time_datafusion.parquet` |
| `combined_balanced_dataset` | `combined_configurations_balanced_duckdb.parquet` and `combined_configurations_balanced_datafusion.parquet` |

Each row is one dataset and configuration. `output_file_name` is the file written with the configuration, and `configuration` is its label in the figure:
- **Storage size, ingestion time and query time:** `TOP-1` to `TOP-3`, the three best configurations by that metric, ranked by DuckDB or DataFusion query time for the query-time datasets. Ingestion times are stored in whole seconds, so ties are broken with the fractional time.
- **Balanced:** for each target, the best configuration and the balanced one:
  - `best_ingestion`, `best_storage`, `best_query_time_datafusion` and `best_query_time_duckdb`: the configuration with the lowest ingestion time, storage size, or DataFusion or DuckDB query time, the same as TOP-1.
  - `balance_ingestion`, `balance_storage`, `balance_query_time_datafusion` and `balance_query_time_duckdb`: the Pareto-optimal configuration chosen by `closest_to_ideal()` in [compute_pareto.py](compute_pareto.py), with weight 0.5 on the target metric and 0.25 on each of the other two.
  - The query metric of `balance_query_time_<engine>` uses that engine's queries. That of `balance_ingestion` and `balance_storage` combines DuckDB and DataFusion, as `compute_pareto.py` does by default.
- **`Default`:** the Rust defaults. The three metrics are relative to it, and the query time is that of DuckDB or DataFusion as named in the file name (DuckDB for storage size and ingestion time).

Re-runs of a configuration are averaged. Configurations are selected by the writer properties stored in `dataset_struct_properties`, not by output file names, because the file names differ between runner versions.

```bash
python create_figure_datasets.py --evaluation-type individual_configurations    # every data/output/<dataset>.db
python create_figure_datasets.py --evaluation-type individual_configurations --database redd=../data/output/redd.db blued=/path/to/blued.db
python create_figure_datasets.py --evaluation-type individual_configurations --system datafusion --output-dir /tmp/figure_datasets
python create_figure_datasets.py --evaluation-type default_writers --native-writers-database ../data/output/native_writers.db \
    --low-level-defaults-database ../data/output/low_level_rust_default_configs.db
```

`--evaluation-type` is required and selects which figures' datasets are written: `default_writers`, `individual_configurations` or `combined_configurations`. The files are written to `figure_datasets/`, or to `--output-dir`. Query times come from DuckDB unless you use `--system datafusion`. Each function also works on its own:

```python
from create_figure_datasets import encodings_dataset
df = encodings_dataset({'redd': '../data/output/redd.db'})
```
