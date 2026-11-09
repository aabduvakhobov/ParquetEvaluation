#!/bin/bash
# This script emulates the default configurations of the C++, Go, Java and Rust Parquet implementations
# with the low-level Rust writer (low-level-writer). For every implementation it:
#   1. applies the matching patch from experiments/writers/rust/patches/<language>_configs.patch,
#   2. rebuilds low-level-writer,
#   3. runs it on each dataset separately,
#   4. reverts the patch.
# At the end the unpatched low-level-writer is rebuilt, so target/release/low-level-writer has its own defaults again.
# The patch is also reverted if the script fails or is interrupted.
#
# Like run_native_writers.sh, the script expects the dataset files in the project root's data/ directory under each
# dataset's name in lower case, e.g. BLUED in `<repo>/data/blued/` and REDD in `<repo>/data/redd/`.
#
# Usage: ./run_low_level_rust_with_default_configs.sh [dataset ...]     (default: blued greend redd neon_wind)
#        e.g. ./run_low_level_rust_with_default_configs.sh redd
#
# All runs are recorded in ONE SQLite database file (tables dataset_struct_properties, file_properties and
# writer_properties of low-level-writer). The output file name <dataset>-<language>.parquet (column
# output_file_name) tells the implementations apart.
#
# Environment variables (all optional):
#   SQLITE_DB     database file for all runs                   (default: experiments/writers/low_level_rust_default_configs.db)
#   OUTPUT_DIR    where the written Parquet files go           (default: experiments/writers/outputs/low_level_rust)
#   KEEP_OUTPUTS  1 keeps the written Parquet files            (default: 0, each file is deleted after its run has been recorded)
#   LANGUAGES     implementations to emulate                   (default: "cpp go java rust")

set -euo pipefail

SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPTS_DIR/../.." && pwd)"
WRITERS_DIR="$REPO_ROOT/experiments/writers"
DATA_DIR="$REPO_ROOT/data"
RUST_DIR="$WRITERS_DIR/rust"
LOW_LEVEL_WRITER="$RUST_DIR/target/release/low-level-writer"

DATASETS="${*:-blued greend redd neon_wind}"
LANGUAGES="${LANGUAGES:-cpp go java rust}"
OUTPUT_DIR="${OUTPUT_DIR:-$WRITERS_DIR/outputs/low_level_rust}"
KEEP_OUTPUTS="${KEEP_OUTPUTS:-0}"

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
LOG_DIR="$OUTPUT_DIR/logs"
mkdir -p "$LOG_DIR"

# Resolve the database to an absolute path once, so every run writes to the same file.
SQLITE_DB="${SQLITE_DB:-$WRITERS_DIR/low_level_rust_default_configs.db}"
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


# 0. Check everything before touching the source code
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
		echo "No Parquet files found in $DATA_DIR/$dataset_name. Download the $dataset_name dataset there first." >&2
		exit 1
	fi
done


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


# 1. Apply each patch, rebuild and run low-level-writer on every dataset
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
		if [ "$KEEP_OUTPUTS" != "1" ]; then
			rm -f -- "${OUTPUT_DIR:?}/${output_name:?}"
		fi
	done
	revert_patch
done


# 2. Rebuild the unpatched writer
log "Rebuilding the unpatched low-level-writer (log: $LOG_DIR/build_unpatched.log)"
build_low_level_writer "$LOG_DIR/build_unpatched.log"

# dataset_struct_properties stores the time in whole seconds, file_properties (one row per column chunk) keeps the fraction.
log "Done. Results in $SQLITE_DB (tables dataset_struct_properties and file_properties):"
python3 - "$SQLITE_DB" <<'EOF' || true
import sqlite3, sys
rows = sqlite3.connect(sys.argv[1]).execute(
    "SELECT dataset_name, output_file_name, MAX(compression_time_in_secs), MAX(file_size_in_bytes) "
    "FROM file_properties GROUP BY file_absolute_path, compression_time_in_secs ORDER BY rowid").fetchall()
print(f"{'dataset':<10} {'output_file_name':<24} {'ingestion_time (s)':>19} {'file_size_in_bytes':>19}")
for dataset, name, seconds, size in rows:
    print(f"{dataset:<10} {name:<24} {seconds:>19.3f} {size:>19}")
EOF
