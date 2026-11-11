#!/bin/bash
# This script emulates the default configurations of the C++, Go, Java and Rust Parquet implementations
# with the low-level Rust writer (low-level-writer) and then runs the range queries on the written files.
#
# 1. Ingestion. For every implementation it:
#      a. applies the matching patch from experiments/writers/rust/patches/<language>_configs.patch,
#      b. rebuilds low-level-writer,
#      c. runs it on each dataset separately, writing <dataset>-<language>.parquet,
#      d. reverts the patch.
#    Afterwards the unpatched low-level-writer is rebuilt, so target/release/low-level-writer has its own defaults
#    again. The patch is also reverted if the script fails or is interrupted.
# 2. Queries. experiments/runner/run_many_queries.py generates the range queries for every written file and runs
#    them with DuckDB and DataFusion.
# 3. The written files are deleted, unless KEEP_OUTPUTS=1. If the queries fail, the files are kept, so the queries
#    can be re-run with SKIP_INGESTION=1.
#
# All written files are kept until the queries have run, so OUTPUT_DIR needs room for every dataset written with
# every implementation (BLUED alone is ~86 GB per implementation).
#
# Like run_native_writers.sh, the script expects the dataset files in the project root's data/ directory under each
# dataset's name in lower case, e.g. BLUED in `<repo>/data/blued/` and REDD in `<repo>/data/redd/`.
#
# Usage: ./run_low_level_rust_with_default_configs.sh [dataset ...]
#   dataset: one or more of blued, greend, redd and neon_wind. Pass only the datasets you have in
#   <repo>/data/; without arguments, all four are expected there.
#   e.g. ./run_low_level_rust_with_default_configs.sh redd
#        ./run_low_level_rust_with_default_configs.sh redd greend
#
# Ingestion and queries are recorded in ONE SQLite database file:
#   - ingestion: tables dataset_struct_properties, file_properties and writer_properties of low-level-writer,
#     where the output file name <dataset>-<language>.parquet (column output_file_name) tells the implementations apart;
#   - queries: tables query, query_schedule (keyed by dataset_name <language>_<dataset>, e.g. cpp_redd) and
#     query_runs (keyed by output_file_name).
#
# Environment variables (all optional):
#   SQLITE_DB       database file for all runs                 (default: data/output/low_level_rust_default_configs.db)
#   OUTPUT_DIR      where the written Parquet files go         (default: experiments/writers/outputs/low_level_rust)
#   KEEP_OUTPUTS    1 keeps the written Parquet files          (default: 0, they are deleted after the queries have run)
#   LANGUAGES       implementations to emulate                 (default: "cpp go java rust")
#   PYTHON          Python with the packages in requirements.txt (default: python3)
#   SKIP_INGESTION  1 only runs the queries, on the files already in OUTPUT_DIR (default: 0)
#   SKIP_QUERIES    1 only ingests                             (default: 0)

set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPTS_DIR/../.." && pwd)"
WRITERS_DIR="$REPO_ROOT/experiments/writers"
DATA_DIR="$REPO_ROOT/data"
RUST_DIR="$WRITERS_DIR/rust"
LOW_LEVEL_WRITER="$RUST_DIR/target/release/low-level-writer"
RUN_MANY_QUERIES="$REPO_ROOT/experiments/runner/run_many_queries.py"

ALL_DATASETS="blued greend redd neon_wind"
DATASETS="${*:-$ALL_DATASETS}"
for dataset_name in $DATASETS; do
	case " $ALL_DATASETS " in
		*" $dataset_name "*) ;;
		*) echo "Unknown dataset: $dataset_name (expected one or more of: $ALL_DATASETS)" >&2; exit 1 ;;
	esac
done
LANGUAGES="${LANGUAGES:-cpp go java rust}"
OUTPUT_DIR="${OUTPUT_DIR:-$WRITERS_DIR/outputs/low_level_rust}"
KEEP_OUTPUTS="${KEEP_OUTPUTS:-0}"
PYTHON="${PYTHON:-python3}"
SKIP_INGESTION="${SKIP_INGESTION:-0}"
SKIP_QUERIES="${SKIP_QUERIES:-0}"

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
LOG_DIR="$OUTPUT_DIR/logs"
mkdir -p "$LOG_DIR"

# Resolve the database to an absolute path once, so every run writes to the same file.
SQLITE_DB="${SQLITE_DB:-$REPO_ROOT/data/output/low_level_rust_default_configs.db}"
mkdir -p "$(dirname "$SQLITE_DB")"
SQLITE_DB="$(cd "$(dirname "$SQLITE_DB")" && pwd)/$(basename "$SQLITE_DB")"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

get_patch_file() {
	case "$1" in
		cpp|go|java|rust) echo "$RUST_DIR/patches/$1_configs.patch" ;;
		*) echo "unknown implementation language: $1 (expected cpp, go, java or rust)" >&2; exit 1 ;;
	esac
}

build_low_level_writer() {
	local log_file=$1
	if ! (cd "$RUST_DIR" && cargo build --release --bin low-level-writer) > "$log_file" 2>&1; then
		echo "Building low-level-writer failed, see $log_file" >&2
		exit 1
	fi
}


# Written files, in the order they are written: <dataset>-<language>.parquet
output_files=()
for language in $LANGUAGES; do
	get_patch_file "$language" > /dev/null
	for dataset_name in $DATASETS; do
		output_files+=("$OUTPUT_DIR/$dataset_name-$language.parquet")
	done
done


# 0. Check everything before touching the source code
if [ "$SKIP_QUERIES" != "1" ]; then
	if ! "$PYTHON" -c "import pyarrow, duckdb, datafusion, polars" 2>/dev/null; then
		echo "$PYTHON cannot import the query packages. Install requirements.txt (pip install -r $REPO_ROOT/requirements.txt)" \
			"or point PYTHON to a Python that has them." >&2
		exit 1
	fi
fi
if [ "$SKIP_INGESTION" != "1" ]; then
	for language in $LANGUAGES; do
		patch_file="$(get_patch_file "$language")"
		if (cd "$REPO_ROOT" && git apply -R --check --whitespace=nowarn "$patch_file" 2>/dev/null); then
			echo "$patch_file is already applied. Revert it first: (cd $REPO_ROOT && git apply -R $patch_file)" >&2
			exit 1
		fi
		if ! (cd "$REPO_ROOT" && git apply --check --whitespace=nowarn "$patch_file"); then
			echo "$patch_file does not apply to the current source in $RUST_DIR/src" >&2
			exit 1
		fi
	done
	for dataset_name in $DATASETS; do
		if ! compgen -G "$DATA_DIR/$dataset_name/*.parquet" > /dev/null; then
			echo "No Parquet files found in $DATA_DIR/$dataset_name. Download the $dataset_name dataset there first, or pass only the datasets you have, e.g. $0 redd." >&2
			exit 1
		fi
	done
else
	for output in "${output_files[@]}"; do
		if [ ! -f "$output" ]; then
			echo "SKIP_INGESTION=1 but $output does not exist. Run the ingestion first." >&2
			exit 1
		fi
	done
fi


# Revert the applied patch on success, failure and Ctrl+C, so the source is never left patched.
applied_patch=""
revert_patch() {
	if [ -n "$applied_patch" ]; then
		(cd "$REPO_ROOT" && git apply -R --whitespace=nowarn "$applied_patch")
		log "Reverted $(basename "$applied_patch")"
		applied_patch=""
	fi
}
trap revert_patch EXIT
trap 'exit 130' INT TERM


# 1. Ingestion: apply each patch, rebuild and run low-level-writer on every dataset
if [ "$SKIP_INGESTION" != "1" ]; then
	for language in $LANGUAGES; do
		patch_file="$(get_patch_file "$language")"
		(cd "$REPO_ROOT" && git apply --whitespace=nowarn "$patch_file")
		applied_patch="$patch_file"
		log "Applied $(basename "$patch_file"), building low-level-writer (log: $LOG_DIR/build_$language.log)"
		build_low_level_writer "$LOG_DIR/build_$language.log"

		for dataset_name in $DATASETS; do
			dataset_dir="$DATA_DIR/$dataset_name"
			output_name="$dataset_name-$language.parquet"
			log "[$language] $dataset_name: $dataset_dir -> $OUTPUT_DIR/$output_name"
			# Usage: low-level-writer table_name error_bound /path/to/parquet_files /path/to/save unique_file_name /path/to/sqlite_db KEY=VALUE ...
			if ! "$LOW_LEVEL_WRITER" "$dataset_name" "0" "$dataset_dir" "$OUTPUT_DIR" "$output_name" "$SQLITE_DB" \
					"COMPRESSION=UNCOMPRESSED" > "$LOG_DIR/$dataset_name-$language.log" 2>&1; then
				echo "low-level-writer with the $language defaults failed on $dataset_name, see $LOG_DIR/$dataset_name-$language.log" >&2
				exit 1
			fi
		done
		revert_patch
	done

	# Rebuild the unpatched writer
	log "Rebuilding the unpatched low-level-writer (log: $LOG_DIR/build_unpatched.log)"
	build_low_level_writer "$LOG_DIR/build_unpatched.log"
fi


# 2. Queries on every written file, recorded in the same database
if [ "$SKIP_QUERIES" != "1" ]; then
	log "Running the queries on ${#output_files[@]} files (log: $LOG_DIR/queries.log)"
	# Started from LOG_DIR, so files the query engines write to the working directory end up next to the logs.
	if ! (cd "$LOG_DIR" && "$PYTHON" "$RUN_MANY_QUERIES" --database "$SQLITE_DB" --log-file "$LOG_DIR/queries.log" \
			"${output_files[@]}"); then
		echo "The queries failed, see $LOG_DIR/queries.log. The written files are kept in $OUTPUT_DIR;" \
			"re-run the queries with SKIP_INGESTION=1." >&2
		exit 1
	fi
fi


# 3. Delete the written files
if [ "$KEEP_OUTPUTS" != "1" ] && [ "$SKIP_QUERIES" != "1" ]; then
	for output in "${output_files[@]}"; do
		rm -f -- "${output:?}"
	done
fi


# dataset_struct_properties stores the time in whole seconds, file_properties (one row per column chunk) keeps the fraction.
log "Done. Results in $SQLITE_DB:"
python3 - "$SQLITE_DB" <<'EOF' || true
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
if "file_properties" in tables:
    rows = db.execute(
        "SELECT dataset_name, output_file_name, MAX(compression_time_in_secs), MAX(file_size_in_bytes) "
        "FROM file_properties GROUP BY file_absolute_path, compression_time_in_secs ORDER BY rowid").fetchall()
    print("Ingestion (table file_properties):")
    print(f"{'dataset':<10} {'output_file_name':<24} {'ingestion_time (s)':>19} {'file_size_in_bytes':>19}")
    for dataset, name, seconds, size in rows:
        print(f"{dataset:<10} {name:<24} {seconds:>19.3f} {size:>19}")
if "query_runs" in tables:
    rows = db.execute(
        "SELECT output_file_name, system_name, COUNT(*), AVG(execution_time_ms) FROM query_runs "
        "GROUP BY output_file_name, system_name ORDER BY output_file_name, system_name").fetchall()
    # run_queries.py stores nanoseconds / 1000, i.e. microseconds, in the execution_time_ms column.
    print("Queries (table query_runs, execution_time_ms holds microseconds):")
    print(f"{'output_file_name':<24} {'engine':<11} {'queries':>8} {'avg_time_us':>19}")
    for name, engine, count, average in rows:
        print(f"{name:<24} {engine:<11} {count:>8} {average:>19.1f}")
EOF
