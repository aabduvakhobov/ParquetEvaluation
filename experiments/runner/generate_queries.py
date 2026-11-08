"""
This script is created to generate SQL queries with a fixed row count (rc). rc starts with 1 and is increased by order of magnitude.
Parameters:
    0. Path to the dataset
    1. Row group size

It works as follows:
    0. Identifies the number of rowgroups, the number of ranges. e.g., if dataset has 1420 then the maximum select range is 1000 rows. 
    1. Selects timestamps for the given range (e.g., if rc=10 then timestamps at index 0 and 9 are selected).
    2. Creates a query using a given query template
    3. Creates all subfolders to store the query using HIVE partitioning scheme
    4. Skips the current rowgroup and creates another query until all rowgroups are traversed

The query is as follows: SELECT SUM(column_x) FROM table WHERE timestamp >= start_range and timestamp < end_range;
It should be possible to change operators like SUM should using parameters.

"""
import math
import sys
from typing import List, Tuple
from dataclasses import dataclass
import random
import datetime
import logging
import time
import sqlite3

import pyarrow
import pyarrow.dataset as ds
import pyarrow.dataset
import pyarrow.parquet
import pyarrow.types
import polars as pl


QUERY_TEMPLATE = """ SELECT SUM("{}") FROM '{{file_path}}' WHERE "TimeStamp" >= TIMESTAMP '{}' AND "TimeStamp" < TIMESTAMP '{}' """
QUERY_SCHEDULE_TABLE_NAME = "query_schedule"
QUERY_TABLE_NAME = "query"


@dataclass
class Dataset:
    name: str
    size: int
    pyarrow_table: pyarrow.Table
    pyarrow_dataset: pyarrow.dataset.dataset
    polars_dataframe: pl.LazyFrame


@dataclass
class Query:
    dataset_name: str
    start_range: int
    end_range: int
    interval: int
    start_time: datetime.datetime
    end_time: datetime.datetime

    def insert_query(self, row_group_size, database_path: str, query_table_name: str):
        # make the database connection with detect_types 
        connection = sqlite3.connect(
            database_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )

        cursor = connection.cursor()
        # create query to insert the data
        insertQuery = """INSERT INTO {}(dataset_name, row_group_size, interval, start_row_group, end_row_group, start_time, end_time, created_at)
            VALUES (?,?,?,?,?,?,?,?);""".format(query_table_name)
        # TODO: populate the rest of the row
        row_to_insert = [
            self.dataset_name,
            row_group_size,
            self.interval,
            self.start_range,
            self.end_range,
            self.start_time,
            self.end_time,
            str(datetime.datetime.now())
        ]
        # insert the data into table
        cursor.execute(insertQuery, row_to_insert)
        # commit the changes,
        # close the cursor and database connection 
        connection.commit()
        cursor.close()
        connection.close()


def create_query_table(database_path, query_table_name=QUERY_TABLE_NAME):
        create_query_table_statement = """
        CREATE TABLE IF NOT EXISTS {} (
            id INTEGER PRIMARY KEY,
            dataset_name TEXT,
            row_group_size INT,
            interval INT,
            start_row_group INT,
            end_row_group INT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            created_at TIMESTAMP
        )
        """.format(query_table_name)
        con = sqlite3.connect(database_path)
        cur = con.cursor()
        cur.execute(create_query_table_statement)
        print('Query table was created')
        con.close()


def count_queries(row_group_size, dataset_name, database_path, query_table_name="query"):
        con = sqlite3.connect(database_path)
        cur = con.cursor()
        res = cur.execute(
            "SELECT COUNT(*) FROM {} WHERE dataset_name = ? AND row_group_size = ?".format(query_table_name), 
            (dataset_name, row_group_size)
            ).fetchone()
        con.close()
        return res


def count_query_execution_orders(row_group_size, dataset_name, database_path, query_execution_order_table_name=QUERY_SCHEDULE_TABLE_NAME):
        con = sqlite3.connect(database_path)
        cur = con.cursor()
        res = cur.execute(
            "SELECT COUNT(*) FROM {} qs JOIN query q ON q.id = qs.query_id WHERE q.dataset_name = ? AND row_group_size = ? ".format(query_execution_order_table_name), 
            (dataset_name, row_group_size)
            ).fetchone()
        con.close()
        return res


def get_value_fields(arrow_table):
    # Ensure the schema only uses supported types.
    fields = []
    for field in arrow_table.schema:
        if field.type == pyarrow.float32():
            fields.append(field)
    return fields


def get_timestamp_field(arrow_dataset) -> List[pyarrow.Field]:
    # Ensure the schema only uses supported types.
    fields = []
    for field in arrow_dataset.schema:
        if field.type == pyarrow.types.is_integer(field.type) or pyarrow.types.is_temporal(field.type):
            fields.append(field)
    return fields


def get_number_of_row_groups(table: pyarrow.Table, row_group_size: int):
    num_row_groups = int(table.num_rows / row_group_size)
    return num_row_groups


def get_query_intervals(dataset_size):
    intervals = []
    increment = 1
    scale_factor = 10
    while dataset_size > increment:
        intervals.append(increment)
        increment *= scale_factor
    return intervals


def read_dataset(path_to_parquet_file_files: str, dataset_name: str):
    if  "blued" in dataset_name:
        # we lazily read large dataset with polars
        polars_df = pl.scan_parquet(path_to_parquet_file_files)
    else:
        polars_df = None
    pyarrow_dataset = ds.dataset(path_to_parquet_file_files)
    # we load other datasets than blued to the memory to speed up random lookups
    pyarrow_table = pyarrow_dataset.to_table() if "blued" not in dataset_name else None

    dataset = Dataset (
        name=dataset_name,
        size=pyarrow_dataset.count_rows(),
        pyarrow_dataset=pyarrow_dataset,
        pyarrow_table=pyarrow_table,
        polars_dataframe=polars_df
    )
    return dataset


def get_start_end_time(dataset: Dataset, start_index:int, end_index:int, timestamp_col_name:str):
    if "blued" in dataset.name:
        start_time = str(dataset.polars_dataframe.slice(start_index, 1).collect().row(0)[0])
        end_time = str(dataset.polars_dataframe.slice(end_index, 1).collect().row(0)[0])
    else:
        start_time = str(dataset.pyarrow_table[timestamp_col_name][start_index].as_py())
        end_time = str(dataset.pyarrow_table[timestamp_col_name][end_index].as_py())

    return start_time, end_time


def create_randomized_index_ranges(
    n: int,
    interval: int,
    row_group_size: int,
    dataset_size: int,
    rng: random.Random,
) -> List[Tuple[int, int]]:
    # this function creates a possible list of all start and end indeces for the given dataset size and row group size
    # then it randomly selects `n` number of tuples from this range
    ranges = []
    start_index = 0
    end_index = start_index + interval
    row_group_num = 0
    # `save_index` and `next_index` are used for the query string to avoid overlapping row groups during the execution 
    # update the start_index so:
    # 1) it does not overlap with previous end_index and 
    # 2) is on a new row_group
    if row_group_size >= interval:
        while end_index < dataset_size:
            row_group_num += 1
            # row_group_row_count = row_group_num * row_group_size
            ranges.append((start_index, end_index))
            start_index += row_group_size
            end_index += row_group_size
    else:
        while end_index < dataset_size:
            # if the interval is bigger than one row group, the projection will extend to two or more row groups
            # thus we shift the index to the next row group, which comes after the last projected row group 
            projection_range = math.ceil(interval / row_group_size)
            row_group_num += projection_range
            ranges.append((start_index, end_index))
            start_index += projection_range * row_group_size
            end_index = start_index + interval
    # randomize if sample is smaller than query population
    if n < len(ranges):
        ranges = rng.sample(ranges, n)

    return ranges


def generate_queries(
    sample_size_per_interval: int,
    row_group_size: int,
    dataset_name: str,
    path_to_parquet_file_files: str,
    database_path: str,
    logger: logging.Logger,
    seed: int = 42,
):
    rng = random.Random(seed)

    create_query_table(database_path)
    # return from function if queries were already generated and stored
    num_existing_queries = count_queries(row_group_size, dataset_name, database_path)
    if num_existing_queries[0] > 0:
        logger.info(f"Not inserting queries since {num_existing_queries} already exist for dataset_name: {dataset_name} and row_group_size: {row_group_size}")
        return
    tic = time.perf_counter()
    # this function return a list of Query objects 
    dataset = read_dataset(path_to_parquet_file_files, dataset_name)
    intervals = get_query_intervals(dataset.size)
    timestamp_col_name = get_timestamp_field(dataset.pyarrow_dataset)[0].name
    # we only select the first column
    for interval in intervals:
        ranges = create_randomized_index_ranges(
            sample_size_per_interval,
            interval,
            row_group_size,
            dataset.size,
            rng,
        )
        for start_range, end_range in ranges:
            start_time, end_time = get_start_end_time(dataset, start_range, end_range, timestamp_col_name)
            # create query object
            query = Query(
                dataset_name = dataset_name,
                start_range = start_range,
                end_range = end_range,
                interval = interval,
                start_time = start_time,
                end_time = end_time,
            )
            query.insert_query(row_group_size, database_path, QUERY_TABLE_NAME)
    logger.info(f"Inserted all queries in {time.perf_counter()-tic:.4f} secs")


### Functions to create query execution orders
def overlaps(a: dict, b: dict) -> bool:
    """
    Returns True if ranges overlap.

    Examples:
        [0,10] and [2,5]   -> True
        [0,10] and [10,20] -> True
        [0,10] and [11,20] -> False
    """
    return not (a["end_row_group"] < b["start_row_group"] or b["end_row_group"] < a["start_row_group"])


def generate_query_execution_order(
    row_group_size: int,
    table_name: str,
    path_to_query_database: str,
    logger: logging.Logger,
    query_execution_order_table_name=QUERY_SCHEDULE_TABLE_NAME,
    query_table_name=QUERY_TABLE_NAME,
    seed: int = 42,
):
    rng = random.Random(seed)

    create_query_execution_order_table_statement = """
    CREATE TABLE IF NOT EXISTS {} (
        execution_order,
        query_id INTEGER NOT NULL,
        created_at TIMESTAMP,

        FOREIGN KEY(query_id)
        REFERENCES {}(id)
    )
    """.format(query_execution_order_table_name, query_table_name)

    conn = sqlite3.connect(path_to_query_database)
    # create query execution order table if not exists
    cur = conn.cursor()
    cur.execute(create_query_execution_order_table_statement)

    # Ensure query schedules were not created before for the following table
    num_existing_query_execution_orders = count_query_execution_orders(row_group_size, table_name, path_to_query_database)
    if num_existing_query_execution_orders[0] > 0:
        logger.info(f"Not inserting query execution order since {num_existing_query_execution_orders} already exist for dataset_name: {table_name} and row_group_size: {row_group_size}")
        print(f"Not inserting quequery execution order since {num_existing_query_execution_orders} already exist for dataset_name: {table_name} and row_group_size: {row_group_size}")
        return

    rows = cur.execute("""
        SELECT id, start_row_group, end_row_group
        FROM {}
        WHERE row_group_size = ? AND dataset_name = ?
    """.format(query_table_name), (row_group_size, table_name)).fetchall()
    logger.info(f"Retrieved {len(rows)} for dataset: {table_name} and row_group_size: {row_group_size} ")

    # query shuffle logic
    queries = [
        {
            "id": r[0],
            "start_row_group": r[1],
            "end_row_group": r[2],
        }
        for r in rows
    ]

    remaining = queries.copy()

    st = time.perf_counter()    
    result = []
    while remaining:
        if not result:
            result.append(remaining.pop())
            continue

        previous = result[-1]
        # candidates whose ranges do NOT overlap
        candidates = [
            q for q in remaining
            if not overlaps(previous, q)
        ]

        if candidates:
            chosen = rng.choice(candidates)
        else:
            # fallback: unavoidable overlap
            chosen = rng.choice(remaining)

        result.append(chosen)
        remaining.remove(chosen)
    logger.info(f"Finished schedule generation in: {time.perf_counter()-st:.4f} seconds ")

    st = time.perf_counter()
    for order, q in enumerate(result):
        cur.execute("""
            INSERT INTO {} (
                execution_order,
                query_id,
                created_at
            )
            VALUES (?, ?, ?)
        """.format(query_execution_order_table_name), (order, q["id"], datetime.datetime.now()))
    logger.info(f"{time.perf_counter()-st:.4f} seconds spend for insert")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # collect user arguments
    if len(sys.argv) != 6:
        print(f"Usage: {sys.argv[0]} sample_size_per_interval row_group_size table_name /path/to/parquet_file_or_files /path/to/queries_database")
        sys.exit(-1)

    sample_size_per_interval = int(sys.argv[1])
    row_group_size = int(sys.argv[2])
    table_name = sys.argv[3]
    path_to_parquet_file = sys.argv[4]
    path_to_query_database = sys.argv[5]

    logging.basicConfig(
        filename="query_generation.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger(__name__)
    try:
        generate_queries(sample_size_per_interval, row_group_size, table_name, path_to_parquet_file, path_to_query_database, logger)
        print("Query generation is finished")
        logger.info("Query generation is finished")
    except Exception as e:
        logger.exception(f"The script failed in query generation phase")
        print(f"Error: {e}")
        sys.exit(-1)

    try:
        generate_query_execution_order(row_group_size, table_name, path_to_query_database, logger)
        print("Schedule generation is finished")
        logger.info("Schedule generation is finished")
    except Exception as e:
        logger.exception(f"The script failed in query order generation phase")
        print(f"Error: {e}")
        sys.exit(-1)
