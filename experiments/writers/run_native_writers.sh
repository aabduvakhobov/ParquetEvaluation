#!/bin/bash
# This scripts builds Parquet writer programs in C++, Java, Go and Rust.
# Then it runs each of them on the dataset files for BLUED, GREEND, REDD and 2D-Wind (aka neon_wind).
# The script assumes that dataset files are downloaded to the project root's /data repository under each datasets name in lower case
# So for BLUED, it expects the dataset files to be in `/data/blued/` and so on.
# All these writer programs track file size in bytes and ingestion time in seconds.
#
# Usage: ./collect_default_writers_stats.sh [dataset ...]     (default: blued greend redd neon_wind)
#        e.g. ./collect_default_writers_stats.sh redd
#
# Every writer records its run (dataset_name, writer, total_write_time, file_size_in_bytes) in the
# writer_runs table of ONE SQLite database file, which this script passes to all of them.
#
# Environment variables (all optional):
#   SQLITE_DB     database file shared by all writers      (default: experiments/writers/default_writers.db)
#   OUTPUT_DIR    where the written Parquet files go       (default: experiments/writers/outputs)
#   KEEP_OUTPUTS  1 keeps the written Parquet files        (default: 0, each file is deleted after its
#                 run has been recorded, since BLUED alone is ~86 GB per writer)
#   JAVA_TMP_DIR  java.io.tmpdir of the Java writer        (default: $OUTPUT_DIR)
#   WRITERS       writers to build and run                 (default: "cpp go java rust")
#   SKIP_BUILD    1 skips the compilation step             (default: 0)

set -euo pipefail

WRITERS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$WRITERS_DIR/../.." && pwd)"
DATA_DIR="$REPO_ROOT/data"

DATASETS="${*:-blued greend redd neon_wind}"
WRITERS="${WRITERS:-cpp go java rust}"
OUTPUT_DIR="${OUTPUT_DIR:-$WRITERS_DIR/outputs}"
KEEP_OUTPUTS="${KEEP_OUTPUTS:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
JAVA_TMP_DIR="${JAVA_TMP_DIR:-$OUTPUT_DIR}"
LOG_DIR="$OUTPUT_DIR/logs"
mkdir -p "$LOG_DIR"

# One database file for all writers: resolve it to an absolute path once, so every writer
# (each started from a different working directory) opens the same file.
SQLITE_DB="${SQLITE_DB:-$WRITERS_DIR/default_writers.db}"
mkdir -p "$(dirname "$SQLITE_DB")"
SQLITE_DB="$(cd "$(dirname "$SQLITE_DB")" && pwd)/$(basename "$SQLITE_DB")"

CPP_BIN="$WRITERS_DIR/cpp/build/parquet_writer_cpp"
GO_BIN="$WRITERS_DIR/go/parquet-writer-go"
JAVA_JAR="$WRITERS_DIR/java/target/parquet-writer-java-1.0.0-jar-with-dependencies.jar"
RUST_BIN="$WRITERS_DIR/rust/target/release/default-rust-writer"

log() { echo "[$(date '+%H:%M:%S')] $*"; }


# 1. Compile all writers
build_writer() {
	case "$1" in
		cpp)
			# The first configure downloads and builds Arrow C++ (takes a few minutes).
			cmake -DCMAKE_BUILD_TYPE=Release -S "$WRITERS_DIR/cpp" -B "$WRITERS_DIR/cpp/build"
			cmake --build "$WRITERS_DIR/cpp/build" --parallel ;;
		go) (cd "$WRITERS_DIR/go" && CGO_ENABLED=0 go build) ;;
		java) (cd "$WRITERS_DIR/java" && mvn -q package -DskipTests) ;;
		rust) (cd "$WRITERS_DIR/rust" && cargo build --release --bin default-rust-writer) ;;
		*) echo "unknown writer: $1 (expected cpp, go, java or rust)" >&2; exit 1 ;;
	esac
}

if [ "$SKIP_BUILD" != "1" ]; then
	for writer in $WRITERS; do
		log "Building $writer writer (log: $LOG_DIR/build_$writer.log)"
		if ! build_writer "$writer" > "$LOG_DIR/build_$writer.log" 2>&1; then
			echo "Building the $writer writer failed, see $LOG_DIR/build_$writer.log" >&2
			exit 1
		fi
	done
fi


# 2. Run every writer on every dataset 
run_writer() {
	local writer=$1 dataset_dir=$2 output=$3 dataset_name=$4
	case "$writer" in
		cpp) "$CPP_BIN" "$dataset_dir" "$output" "$dataset_name" "$SQLITE_DB" ;;
		go) "$GO_BIN" "$dataset_dir/*.parquet" "$output" "$dataset_name" "$SQLITE_DB" ;;
		java) java -Djava.io.tmpdir="$JAVA_TMP_DIR" -jar "$JAVA_JAR" "$dataset_dir/*.parquet" "$output" "$dataset_name" "$SQLITE_DB" ;;
		rust) "$RUST_BIN" "$dataset_dir" "$output" "$dataset_name" "$SQLITE_DB" ;;
	esac
}

for dataset_name in $DATASETS; do
	dataset_dir="$DATA_DIR/$dataset_name"
	if ! compgen -G "$dataset_dir/*.parquet" > /dev/null; then
		echo "No Parquet files found in $dataset_dir. Download the $dataset_name dataset there first." >&2
		exit 1
	fi
	for writer in $WRITERS; do
		output="$OUTPUT_DIR/${dataset_name}-${writer}.parquet"
		log "[$writer] $dataset_name: $dataset_dir -> $output"
		if ! run_writer "$writer" "$dataset_dir" "$output" "$dataset_name" > "$LOG_DIR/${dataset_name}-${writer}.log" 2>&1; then
			echo "The $writer writer failed on $dataset_name, see $LOG_DIR/${dataset_name}-${writer}.log" >&2
			exit 1
		fi
		if [ "$KEEP_OUTPUTS" != "1" ]; then
			rm -f -- "${output:?}" "$(dirname "${output:?}")/.$(basename "$output").crc"
		fi
	done
done

log "Done. Results in $SQLITE_DB (table writer_runs):"
python3 - "$SQLITE_DB" <<'EOF' || true
import sqlite3, sys
rows = sqlite3.connect(sys.argv[1]).execute(
    "SELECT dataset_name, writer, total_write_time, file_size_in_bytes FROM writer_runs").fetchall()
print(f"{'dataset':<10} {'writer':<6} {'total_write_time (s)':>21} {'file_size_in_bytes':>19}")
for dataset, writer, seconds, size in rows:
    print(f"{dataset:<10} {writer:<6} {seconds:>21.3f} {size:>19}")
EOF
