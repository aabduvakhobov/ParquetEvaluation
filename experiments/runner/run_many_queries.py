"""Generate and run the range queries on Parquet files written outside the experiment runner.

Used by experiments/scripts/run_low_level_rust_with_default_configs.sh, which writes one
<dataset>-<language>.parquet per dataset and emulated implementation and then passes those files here.
For every file, the queries are generated (generate_queries.py), run with DuckDB and DataFusion
(run_queries.py) and the execution times are stored in the given SQLite database, so ingestion and query
results end up in one database file.

The table name of a file is <language>_<dataset> (e.g. redd-cpp.parquet -> cpp_redd); the query tables
(query, query_schedule, query_runs) are keyed by it.

Usage:
    python run_many_queries.py --database /path/to/results.db [--log-file queries.log] file.parquet [file.parquet ...]
"""
import argparse
import logging
import os
import sys
import time

import pyarrow.parquet as pq

import generate_queries
import run_queries

DATASETS = ["blued", "greend", "redd", "neon_wind"]
QUERY_SAMPLE_SIZE_PER_INTERVAL = 100


def parse_row_group_size(parquet_file_path):
    parquet_file = pq.ParquetFile(parquet_file_path)
    longest_row_group_size = 0
    num_row_groups = parquet_file.num_row_groups
    metadata = parquet_file.metadata

    for i in range(num_row_groups):
        rg = metadata.row_group(i)
        if rg.num_rows > longest_row_group_size:
            longest_row_group_size = rg.num_rows

    return longest_row_group_size


def table_name_for(parquet_file_path):
    """<dataset>-<language>.parquet -> <language>_<dataset>."""
    stem = os.path.basename(parquet_file_path).removesuffix(".parquet")
    dataset, separator, language = stem.partition("-")
    if not separator or dataset not in DATASETS or not language:
        raise ValueError(f"Expected a file named <dataset>-<language>.parquet with dataset one of "
                         f"{', '.join(DATASETS)}, got: {parquet_file_path}")
    return f"{language}_{dataset}"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", required=True, help="SQLite database for the queries and their execution times")
    parser.add_argument("--log-file", default="alternative_writers_query_run.log")
    parser.add_argument("files", nargs="+", help="<dataset>-<language>.parquet files to query")
    args = parser.parse_args()

    files = {}
    for file in args.files:
        if not os.path.isfile(file):
            sys.exit(f"Parquet file not found: {file}")
        files[table_name_for(file)] = os.path.abspath(file)
    database = os.path.abspath(args.database)

    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger(__name__)

    for table_name, file in files.items():
        tic = time.perf_counter()
        row_group_size = parse_row_group_size(file)
        print(f"[{table_name}] {file}: row group size {row_group_size}, generating queries", flush=True)
        generate_queries.generate_queries(
                sample_size_per_interval = QUERY_SAMPLE_SIZE_PER_INTERVAL,
                row_group_size = row_group_size,
                dataset_name = table_name,
                path_to_parquet_file_files = file,
                database_path = database,
                logger = logger
            )
        generate_queries.generate_query_execution_order(
                row_group_size = row_group_size,
                table_name = table_name,
                path_to_query_database = database,
                logger = logger
            )
        logger.info("[%s] Query and schedule generation finished in %.2f seconds",
                    table_name, time.perf_counter() - tic)

        tic = time.perf_counter()
        print(f"[{table_name}] running queries with DuckDB and DataFusion", flush=True)
        run_queries.run_queries(
            row_group_size,
            table_name,
            os.path.basename(file),
            file,
            database,
            True
        )
        logger.info("[%s] Query run finished in %.2f seconds", table_name, time.perf_counter() - tic)


if __name__ == "__main__":
    main()
