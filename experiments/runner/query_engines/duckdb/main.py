import time
import sys
import duckdb
import pandas as pd

def main(query: str, file: str) -> int:
    # filename = file.replace('.parquet', '')
    tic = time.perf_counter_ns()
    res = duckdb.sql(query.format(file_path = file))
    for batch in res.fetch_arrow_table():
        _ = batch.to_pandas()
    # write query result to the file
    tac = time.perf_counter_ns() - tic
    return tac


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f'Usage: {sys.argv[0]} /path/to/parquet_file query_key')
        sys.exit(-1)

    main(sys.argv[1], sys.argv[2])
