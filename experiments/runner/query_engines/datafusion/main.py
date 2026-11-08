# TODO: Time the run and write it to log file with a datasetname
from datafusion import SessionContext, SessionConfig
import sys
import time


def main(query: str, table_name: str, file: str, read_page_index: bool):
    tic = time.perf_counter_ns()
    # Create a DataFusion context
    if read_page_index:
        ctx = SessionContext()
    else:
        config = SessionConfig().set("datafusion.execution.parquet.enable_page_index", "false")
        ctx = SessionContext(config=config)
    # Register table with context
    ctx.register_parquet(table_name, file)
    res = ctx.sql(query.format(file_path = table_name))
    for batch in res.to_arrow_table():
        _ = batch.to_pandas()
    ctx.deregister_table(table_name)
    tac = time.perf_counter_ns() - tic
    return tac


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print(f'Usage: {sys.argv[0]} /path/to/parquet_file query_key')
        sys.exit(-1)

    main(sys.argv[1], sys.argv[2])
