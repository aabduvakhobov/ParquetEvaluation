# TODO: Ensure the same composite key is used to uniquely identify experiment file across tables

import sys
import time
import logging
import sqlite3
from dataclasses import dataclass
import datetime
from typing import List
from dataclasses import dataclass

from query_engines.duckdb.main import main as duckdb_main
from query_engines.datafusion.main import main as datafusion_main


# ENV vars
# Default Parquet Writer parameters
# ERROR_BOUNDS = [0, 0.01, 0.1, 1, 5, 10]
QUERY_RUNS_TABLE_NAME = "query_runs"

@dataclass
class QueryOrder:
    query_id: int
    execution_order: int


def read_query_schedule(dataset_name, row_group_size, query_database_path):
    # Always filter by dataset_name, column_name and row_group_size before retrieving queries
    conn = sqlite3.connect(query_database_path)
    # create scheduler table if not exists
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT qs.query_id, qs.execution_order 
        FROM query_schedule qs 
        JOIN query q ON q.id = qs.query_id 
        WHERE q.dataset_name = ? AND q.row_group_size = ?
    """, (dataset_name, row_group_size) ).fetchall()
    query_orders = []
    for row in rows:
        query_orders.append(QueryOrder(int(row[0]), int(row[1])))
    return query_orders


def read_query_str(execution_order, dataset_name, row_group_size, query_database_path):
    # Always filter by dataset_name, column_name and row_group_size before retrieving queries
    connection = sqlite3.connect(
        query_database_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    select_schedules_query = """
    SELECT q.query_string from query q 
    JOIN query_schedule qs ON q.id = qs.query_id 
    WHERE qs.execution_order = ? AND q.dataset_name = ? AND q.row_group_size = ?;
    """

    cursor = connection.cursor()
    # insert the data into table
    query = cursor.execute(select_schedules_query, (execution_order, dataset_name, row_group_size)).fetchone()
    # commit the changes,
    # close the cursor and database connection
    cursor.close()
    connection.close()
    return query


# returns column names to run queries on. If dataset is multivariate time series,
# then column names are selected based on the lowest and highest standard deviation
def get_value_column_names(dataset_name:str) -> List[str]:
    if 'parklog' in dataset_name:
        return ["AvailablePower", "Frequency"]
    elif 'redd' in dataset_name:
        return ["waveform_values_amps"]    
    elif 'blued' in dataset_name:
        return ["current_a", "voltage"]
    elif 'turbinelog' in dataset_name:
        return ["Frequency", "ActivePower"]
    elif 'voltagelog' in dataset_name:
        return ["ReactivePower", "Voltage"]
    elif 'm1data' in dataset_name:
        return ["High", "Open"]
    elif 'tickdata' in dataset_name:
        return ["Bid", "Ask"]
    elif 'wind' == dataset_name:
        return ["active_power", "sin_wind_dir"]
    elif 'neon' in dataset_name or '2d_wind' in dataset_name:    
        return ['windSpeedMinimum', 'windSpeedMaximum']
    elif 'greend' in dataset_name:
        return ['fridge', 'washing_machine']
    else:
        raise ValueError(f"Cannot find column name for the given dataset name: {dataset_name}")


def get_timestamp_column_name(dataset_name):
    if 'parklog' in dataset_name or 'voltagelog' in dataset_name or 'turbinelog' in dataset_name:
        return "TimeStamp"
    elif 'redd' in dataset_name or 'blued' in dataset_name:
         return "time"
    elif 'm1data' in dataset_name or 'tickdata' in dataset_name:
         return "DateTime"
    elif 'wind' == dataset_name:
         return "datetime"
    elif 'neon' in dataset_name or '2d_wind' in dataset_name:    
        return "endDateTime"
    elif "greend" in dataset_name:
        return "timestamp"
    else:
        raise ValueError(f"Cannot find timestamp column name for the given dataset name: {dataset_name}")


def construct_query_with_start_end_dates(execution_order:int, dataset_name:str, row_group_size:int, value_column_name:str, cursor:sqlite3.Cursor):
    # Always filter by dataset_name, column_name and row_group_size before retrieving queries
    select_schedules_query = """
    SELECT q.start_time, q.end_time from query q 
    JOIN query_schedule qs ON q.id = qs.query_id 
    WHERE qs.execution_order = ? AND q.dataset_name = ? AND q.row_group_size = ?;
    """
    # insert the data into table
    dates = cursor.execute(select_schedules_query, (execution_order, dataset_name, row_group_size)).fetchone()
    QUERY_TEMPLATE = """ SELECT SUM("{}") FROM '{{file_path}}' WHERE "{}" >= TIMESTAMP '{}' AND "{}" <= TIMESTAMP '{}' """
    timestamp_column_name = get_timestamp_column_name(dataset_name)
    query_str = QUERY_TEMPLATE.format(value_column_name, timestamp_column_name, str(dates[0]), timestamp_column_name, str(dates[1]))
    return query_str


def create_query_runs_table(database_path, table_name = QUERY_RUNS_TABLE_NAME):
    create_table_statement = """
    CREATE TABLE IF NOT EXISTS {} (
        output_file_name TEXT,
        query_id INT,
        column_name TEXT,
        system_name TEXT,
        execution_time_ms INT, 
        created_at TEXT,

        FOREIGN KEY(query_id)
        REFERENCES query(id)
    ) STRICT
    """.format(table_name)
    conn = sqlite3.connect(database_path)
    # create scheduler table if not exists
    cur = conn.cursor()
    cur.execute(create_table_statement)
    conn.close()


def insert_execution_time(ouput_file_name: str, query_id: int, execution_time: int, value_col_name:str, system_name: str, cursor: sqlite3.Cursor, query_table_name = QUERY_RUNS_TABLE_NAME):
    cursor.execute("""
                INSERT INTO {} (
                output_file_name,
                query_id,
                column_name,
                system_name,
                execution_time_ms,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """.format(query_table_name), (ouput_file_name, query_id, value_col_name, system_name, execution_time, str(datetime.datetime.now())))


def run_queries(row_group_size, table_name, output_file_name, output_file_path, queries_database, datafusion_read_page_index = False):
    create_query_runs_table(queries_database)
    # Run queries
    # run only once
    # returns a list of QueryOrder objects
    query_orders = read_query_schedule(table_name, row_group_size, queries_database)
    # get value columns to run queries on
    value_column_names = get_value_column_names(table_name)
    # commit only after all inserts are performed
    connection = sqlite3.connect(queries_database)
    cursor = connection.cursor()
    if len(query_orders) == 0:
        raise RuntimeError(f"0 queries found for the given: row_group_size: {row_group_size} and table_name: {table_name}")
    for value_column_name in value_column_names:
        for query_order in query_orders:
            query_str = construct_query_with_start_end_dates(query_order.execution_order, table_name, row_group_size, value_column_name, cursor)
            # run duckdb
            # extra: get count num rows
            execution_time = duckdb_main(query_str, output_file_path)
            insert_execution_time(output_file_name, query_order.query_id, int(execution_time/1000), value_column_name, 'duckdb', cursor)
            # run datafusion
            # read_page_index = False  as we are testing the worst case so it is fine to disable page skipping. In the main experiment runner file `test_parameters_in_isolation.py`, it is 
            # identified based on writer properties
            execution_time = datafusion_main(query_str, table_name, output_file_path, datafusion_read_page_index)
            insert_execution_time(output_file_name, query_order.query_id, int(execution_time/1000), value_column_name, 'datafusion', cursor)

    cursor.close()
    connection.commit()
    connection.close()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} row_group_size dataset_name path/to/parquet_file /path/to/query_database")
        sys.exit(-1)

    # capture command line arguments
    row_group_size = int(sys.argv[1])
    dataset_name = sys.argv[2]
    path_to_parquet_file = sys.argv[3]
    path_to_query_database = sys.argv[4]

    tic = time.monotonic()
    output_file_name = path_to_parquet_file.split('/')[-1]
    run_queries(row_group_size, dataset_name, output_file_name, path_to_parquet_file, path_to_query_database)

    logging.basicConfig(
        filename="query_read.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Completed all queries for: {path_to_parquet_file}")
    elapsed = time.monotonic() - tic
    logger.info("Experiment finished in %.2f seconds", elapsed)
