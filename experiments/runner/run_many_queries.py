import logging
import os
import sys
import pyarrow.parquet as pq
import time

import run_queries
import generate_queries


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


if __name__ == "__main__":
    # Directories holding the files written by the default C++/Go/Java writers,
    # one <dataset>.parquet per dataset (see experiments/scripts/run_default_writers.sh).
    writer_dirs = {
        "cpp": os.environ.get("CPP_SAVE_DIR"),
        "go": os.environ.get("GO_SAVE_DIR"),
        "java": os.environ.get("JAVA_SAVE_DIR"),
    }
    missing = [f"{w.upper()}_SAVE_DIR" for w, d in writer_dirs.items() if not d]
    if missing:
        sys.exit(f"Set the following environment variables: {', '.join(missing)}")

    logging.basicConfig(
        filename="alternative_writers_query_run.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger(__name__)

    datasets = ["blued", "greend", "redd", "neon_wind"]
    files_path_list = {
        f"{writer}_{dataset}": os.path.join(path, f"{dataset}.parquet")
        for writer, path in writer_dirs.items()
        for dataset in datasets
    }
    tic = time.perf_counter()

    for table_name, file in files_path_list.items():
        row_group_size = parse_row_group_size(file)
        print(f"file: {file}, {row_group_size}")
        generate_queries.generate_queries(
                sample_size_per_interval = 100,
                row_group_size = row_group_size,
                dataset_name = table_name,
                path_to_parquet_file_files = file, 
                database_path = "other_writers.db",
                logger = logger
            )
        generate_queries.generate_query_execution_order(
                row_group_size = row_group_size,
                table_name = table_name,
                path_to_query_database= "other_writers.db",
                logger = logger
            )
        query_generation_end = time.perf_counter()

        logger.info("Query and schedule generation finished in %.2f seconds", query_generation_end - tic)
        tic = time.perf_counter()
        run_queries.run_queries(
            row_group_size,
            table_name,
            table_name,
            file,
            "other_writers.db",
            True
        )
        logger.info("Query run finished in %.2f seconds", time.perf_counter() - tic)
