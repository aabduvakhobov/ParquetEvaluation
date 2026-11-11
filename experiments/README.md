# Running Experiments

```
experiments/
├── writers/          Parquet writers
│   ├── rust/         low-level configurable writer (low-level-writer) + default writer + patches
│   ├── cpp/          default Arrow C++ writer
│   ├── go/           default Arrow Go writer
│   └── java/         default parquet-java writer
├── runner/           Python experiment runner (ingestion -> query generation -> query execution)
└── scripts/          shell scripts for the default-writer experiments
```

## 1. Build the writers

| Writer | Build | Binary |
|---|---|---|
| Rust | `cd writers/rust && cargo build --release` | `target/release/{low-level-writer,default-rust-writer}` |
| C++ | `cd writers/cpp && cmake -DCMAKE_BUILD_TYPE=Release -S . -B build && cmake --build build` | `build/parquet_writer_cpp` |
| Go | `cd writers/go && CGO_ENABLED=0 go build` | `parquet-writer-go` |
| Java | `cd writers/java && mvn package` | `target/parquet-writer-java-1.0.0-jar-with-dependencies.jar` |

The C++ build downloads and compiles the Arrow C++ version used in the paper by itself, so the first build takes a while and needs internet. It produces a fully static binary that runs on any x86-64 Linux machine (see [writers/cpp/README.md](writers/cpp/README.md)). The Java writer needs **Java 21** (see [writers/java/README.md](writers/java/README.md)).

## 2. Place the datasets

`run_native_writers.sh`, `run_low_level_rust_with_default_configs.sh` and `generate_configurations.py` read every dataset from `<repo>/data/<dataset>/`: `data/blued/`, `data/greend/`, `data/redd/` and `data/neon_wind/`. Each directory holds that dataset's Parquet files. See [Datasets](../README.md#datasets) for the download.

## 3. Experiments

### Default configurations of the Parquet implementations

```bash
./scripts/run_native_writers.sh redd                        # build all writers, run them on data/<dataset>/
./scripts/run_low_level_rust_with_default_configs.sh redd   # C++/Go/Java/Rust defaults emulated in low-level-writer, then queried
./scripts/run_default_writers.sh            # cpp go java rust; ingestion times in scripts/default_writers.logs
```

The C++, Go, Java and Rust (`default-rust-writer`) writers take two more arguments, the dataset name and a SQLite database path:

```
<writer> <input> <output.parquet> <dataset-name> <sqlite-database>
```

Each run appends one row to the `writer_runs` table, which the writer creates if it does not exist:

| Column | Description |
|---|---|
| `dataset_name` | Dataset name passed on the command line. |
| `writer` | `cpp`, `go`, `java` or `rust`. |
| `total_write_time` | Seconds spent reading the input files and writing the Parquet file. |
| `file_size_in_bytes` | Size of the written Parquet file. |

[run_native_writers.sh](scripts/run_native_writers.sh) first compiles all four writers. It then runs each one on the Parquet files in `<repo>/data/<dataset>/`, and passes all of them the same database file, `<repo>/data/output/native_writers.db`, or `$SQLITE_DB` if set. By default the written Parquet files are deleted once their run is recorded; set `KEEP_OUTPUTS=1` to keep them in `writers/outputs/`. The other options are listed at the top of the script.

[run_low_level_rust_with_default_configs.sh](scripts/run_low_level_rust_with_default_configs.sh) emulates each implementation's defaults with `low-level-writer`, then measures query times on the files it wrote. It works in three steps:

1. **Ingestion.** For each patch in [writers/rust/patches/](writers/rust/patches/) (`cpp`, `go`, `java`, `rust`), it applies the patch, rebuilds the writer, writes `<dataset>-<language>.parquet` from every dataset in `<repo>/data/<dataset>/`, then reverts the patch. The patch is also reverted if a run fails or you press Ctrl+C, and the unpatched writer is rebuilt at the end.
2. **Queries.** [run_many_queries.py](runner/run_many_queries.py) generates the range queries for every written file and runs them with DuckDB and DataFusion.
3. **Cleanup.** It deletes the written files unless `KEEP_OUTPUTS=1` is set.

Ingestion and queries go into one database, `<repo>/data/output/low_level_rust_default_configs.db`, or `$SQLITE_DB` if that is set:
- **Ingestion** goes to `dataset_struct_properties` and `file_properties`, where `output_file_name` names the implementation.
- **Queries** go to `query` and `query_schedule`, keyed by `<language>_<dataset>`, e.g. `cpp_redd`, and to `query_runs`, keyed by `output_file_name`.

Options:
- `LANGUAGES="cpp go"` runs a subset of the patches.
- `PYTHON` selects a Python that has the packages in [requirements.txt](../requirements.txt). The script checks this before it changes anything.
- If the queries fail, the written files are kept, and `SKIP_INGESTION=1` re-runs only the queries on them.
- `SKIP_QUERIES=1` only ingests.

All written files stay on disk until the queries are done, so the output directory needs room for every dataset written once per implementation. BLUED alone takes about 86 GB per implementation.

### Individual and combined parameters
Example use with REDD dataset assuming you already installed all Python dependencies as described in [Python packages](../README.md#python-packages):
```bash
cd runner

# activate Python environment
conda activate parquet-evaluation

# generate configurations for testing Parquet parameters in isolation
python generate_configurations.py --dataset-name redd --evaluation-type individual_parameters
# start experiment
python test_parameters_in_isolation.py MANUAL        # runs the generated configurations.yaml
python test_parameters_in_isolation.py DEFAULT       # low-level Rust writer with its default properties
```

[generate_configurations.py](runner/generate_configurations.py) writes `configurations.yaml`. The dataset is read from `<repo>/data/<dataset>/`, for example `data/redd/` for `--dataset-name redd`. The written Parquet files go to `--path-to-save`, by default `writers/outputs/save_data/`. The results go to `--path-to-metadata-database`, by default `<repo>/data/output/<dataset>.db`, where [visualization/create_figure_datasets.py](../visualization/create_figure_datasets.py) reads them.

With `individual_parameters`, each parameter is varied on its own against a baseline: the Rust defaults plus dictionary encoding and PAGE statistics. This yields 27 configurations per dataset; a configuration that equals one already listed (such as the baseline) is listed once.
- **Block compressors:** GZIP 1/9, Brotli 1/11, Zstd 1/22, Snappy, LZ4, uncompressed.
- **Data and dictionary page sizes:** 1 KiB to 1 MiB.
- **Value encodings:** BYTE_STREAM_SPLIT, RLE_DICTIONARY, PLAIN.
- **Timestamp encodings:** DELTA_BINARY_PACKED, RLE_DICTIONARY, PLAIN.
- **Bloom filter, offset index and statistics level.**
- **Row group sizes:** chosen per dataset.

In `individual_parameters`, RLE_DICTIONARY means `dictionary_enabled: true`, and every other encoding is run with dictionary encoding off.

parquet-rs always writes the offset index while PAGE statistics are on. Both offset index configurations, enabled and disabled, therefore use no statistics.

With `combined_parameters`, the configurations are every combination of the values kept after pruning. The values are the same for all datasets:
- **Row group sizes:** 102,400, 1,048,576 and 10,240,000 rows.
- **Data page size:** 1 MiB.
- **Dictionary page sizes:** 10 KiB and 1 MiB.
- **Block compressors:** Brotli 1/4/7, LZ4, uncompressed, Zstd 1/4/7/10/13.
- **Value encodings:** BYTE_STREAM_SPLIT, RLE_DICTIONARY, PLAIN.
- **Timestamp encodings:** DELTA_BINARY_PACKED, RLE_DICTIONARY, PLAIN.
- **Statistics:** NONE, CHUNK, PAGE.

The bloom filter and the offset index are not varied and stay at the runner's defaults. So no bloom filter is written, and the offset index is written only with PAGE statistics.

Dictionary encoding applies to all columns at once, so an encoding pair with RLE_DICTIONARY turns it on for both columns. The other column's encoding becomes its fallback, which the writer switches to once the dictionary page is full. Dictionary encoding is combined with DELTA_BINARY_PACKED but not with BYTE_STREAM_SPLIT. The dictionary page size is only varied while dictionary encoding is on.

Pairs that give the same configuration, such as (RLE_DICTIONARY, PLAIN) and (RLE_DICTIONARY, RLE_DICTIONARY), are run once. That leaves 720 configurations per dataset.

On top of these, eight hand-picked configurations are run with Brotli 11 and Zstd 22 each, adding 16 more for a total of 736 per dataset. They are listed in `COMBINED_EXTRA_CONFIGURATIONS` in [generate_configurations.py](runner/generate_configurations.py). Settings they don't list keep the runner's defaults: a row group of 1,048,576 rows, a 1 MiB data page, dictionary off and no statistics.

```bash
python generate_configurations.py --dataset-name redd --evaluation-type combined_parameters
python test_parameters_in_isolation.py MANUAL
```

You can also write `configurations.yaml` by hand from [configurations_example.yaml](runner/configurations_example.yaml). Its `isolated` section is run with `python test_parameters_in_isolation.py ISOLATED`.

Before anything else, `test_parameters_in_isolation.py` runs `cargo build --release` in [writers/rust](writers/rust/), so the experiments always use a writer built from the current source. It stops if the build fails.

For every configuration, the runner:
1. writes the dataset with `low-level-writer` and records the ingestion time and file size in the SQLite database (`path_to_metadata_database`);
2. generates range queries per column and interval ([generate_queries.py](runner/generate_queries.py));
3. runs them with DuckDB and DataFusion ([query_engines/](runner/query_engines/)) and stores the execution times in the same database.

`path_to_executable` is `experiments/writers/rust/target/release/low-level-writer`.
The resulting databases are the input of [visualization/](../visualization/).

The writer parameters are documented in [writers/rust/README.md](writers/rust/README.md).
