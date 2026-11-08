import logging
import pyarrow.parquet as pq
import time
import sys

import run_queries
import generate_queries

query_database_path = 'rust_implementations_query_times.db'

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
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} path/to/parquet_file/")
        sys.exit(-1)

    PATH = sys.argv[1]

    logging.basicConfig(
        filename="alternative_writers_query_run.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger(__name__)

    files_path_list = {
#        "java_blued": PATH + 'blued-java.parquet',
#        "cpp_blued": PATH + 'blued-cpp.parquet',        
#        "go_blued": PATH + 'blued-go.parquet',        
#        "java_redd": PATH + 'redd-java.parquet',
#        'cpp_redd': PATH + 'redd-cpp.parquet',
#        'go_redd': PATH + 'redd-go.parquet',
#        "java_greend": PATH + 'greend-java.parquet',
#        'cpp_greend': PATH + 'greend-cpp.parquet',
#        'go_greend': PATH + 'greend-go.parquet',
        "java_neon_wind": PATH + '2d_wind-java.parquet',
        'cpp_neon_wind': PATH + '2d_wind-cpp.parquet',
        'go_neon_wind': PATH + '2d_wind-go.parquet'
        }
    tic = time.perf_counter()

    for table_name, file in files_path_list.items():
        row_group_size = parse_row_group_size(file)
        print(f"file: {file}, {row_group_size}")
        try:
            generate_queries.generate_queries(
                sample_size_per_interval = 100,
                row_group_size = row_group_size,
                dataset_name = table_name,
                path_to_parquet_file_files = file, 
                database_path = query_database_path,
                logger = logger
            )
        except Exception as e:
            logger.exception(f"The script failed in query generation phase: {e}")
            print(f"Error: {e}")
            sys.exit(-1)

        try:
            generate_queries.generate_query_execution_order(
                row_group_size = row_group_size,
                table_name = table_name,
                path_to_query_database = query_database_path,
                logger = logger
            )
        except Exception as e:
            logger.exception(f"The script failed in query order generation phase: {e}")
            print(f"Error: {e}")
            sys.exit(-1)
        query_generation_end = time.perf_counter()

        logger.info("Query and schedule generation finished in %.2f seconds", query_generation_end - tic)
        tic = time.perf_counter()
        try:
            run_queries.run_queries(
            row_group_size,
            table_name,
            table_name,
            file,
            query_database_path,
            True
            )
        except Exception as e:
            logger.exception(f"The script failed in query generation phase: {e}")
            print(f"Error: {e}")
            sys.exit(-1)
        logger.info("Query run finished in %.2f seconds", time.perf_counter() - tic)

