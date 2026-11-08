# TODO: Ensure the same composite key is used to uniquely identify experiment file across tables

import json
import sys
import subprocess
import socket
import datetime
import os
import time
from pathlib import Path
import logging
import yaml
from dataclasses import dataclass, asdict, fields, replace
from typing import List
from enum import Enum

from utils import get_git_commit_hash, ZstdLevel, BrotliLevel
from generate_queries import generate_queries, generate_query_execution_order
from run_queries import run_queries

# Default Parquet Writer parameters

RELATIVE_ERROR_BOUND = True
COMPRESSION = { 'zstd': "ZSTD", "brotli": 'BROTLI', 'lz4':'LZ4', 'uncompressed':'UNCOMPRESSED', 'gzip':'GZIP', 'snappy':'SNAPPY' }
VALUE_ENCODING = { 'plain':"PLAIN", 'byte_stream_split' : 'BYTE_STREAM_SPLIT' }
TIMESTAMP_ENCODING = { 'plain':"PLAIN", 'delta_binary_packed':'DELTA_BINARY_PACKED' }
STATISTICS_ENABLED = { 'none': "NONE", 'page': 'PAGE', 'chunk':'CHUNK' }
QUERY_RUNS_TABLE_NAME = "query_runs"
QUERY_SAVE_DIR = "./queries"


class ConfigFlag(Enum):
    DEFAULT = 0
    ISOLATED = 1
    MANUAL = 2
    # FACTORIAL = 3


@dataclass
class ParquetWriterProperties:
    bloom_filter_enabled: bool = False
    compression: str = COMPRESSION['uncompressed']
    compression_level: int = 0
    sorting_columns: list = None
    data_page_size_limit: int = 1024*1024
    dictionary_page_size_limit: int = 1024*1024
    dictionary_enabled: bool = False
    max_row_group_size: int = 1024*1024
    timestamp_encoding: str = TIMESTAMP_ENCODING['plain']
    value_encoding: str = VALUE_ENCODING['plain']
    statistics_enabled: bool = STATISTICS_ENABLED['none']
    offset_index_disabled: bool = True


    def spell_for_terminal(self) -> List[str]:
        args: List[str] = []

        for f in fields(self):
            value = getattr(self, f.name)

            # Skip unset / None values
            if value is None:
                continue

            key = f.name.upper()

            # Normalize value
            if isinstance(value, bool):
                value_str = "true" if value else "false"
            elif isinstance(value, list):
                value_str = ",".join(map(str, value))
            else:
                value_str = str(value)

            args.append(f"{key}={value_str}")

        return args


@dataclass
class ExperimentProperties:
    created_at: datetime.datetime
    table_name: str
    source_file_or_directory: str
    executable_path: str
    path_to_save: str
    metadata_database_path: str
    error_bound: float
    node_name: str
    commit_hash: str
    writer_properties: ParquetWriterProperties
    relative_error_bound: bool = RELATIVE_ERROR_BOUND
    
    
    def create_unique_file_name(self) -> str:
        # Generates unique file_name for the parquet file
        # Helper: bool -> "1" / "0"
        def b(v: bool) -> str:
            return "1" if v else "0"

        sorting_columns_str = "None"

        # Build components in FIXED order (matches Rust exactly)
        parts = [
            f"dataset={self.table_name}",
            f"error_bound={self.error_bound}",
            f"bloom={b(self.writer_properties.bloom_filter_enabled)}",
            f"sort={sorting_columns_str}",
            f"page_size={self.writer_properties.data_page_size_limit}",
            f"dictionary_page_size={self.writer_properties.dictionary_page_size_limit}",
            f"dict={b(self.writer_properties.dictionary_enabled)}",
            f"row_group={self.writer_properties.max_row_group_size}",
            f"compression={self.writer_properties.compression}",
            f"comp_lvl={self.writer_properties.compression_level}",
            f"ts_enc={self.writer_properties.timestamp_encoding}",
            f"val_enc={self.writer_properties.value_encoding}",
            f"stats={self.writer_properties.statistics_enabled}",
            f"offset_idx_disabled={b(self.writer_properties.offset_index_disabled)}",
        ]

        # Join and sanitize
        filename = "-".join(parts).replace(" ", "_").replace("/", "_").replace("=","_")

        return f"{filename}.parquet"
    
    
    def spell_out_error_bound(self):
        if self.relative_error_bound:
            return f"{self.error_bound}%"
        else:
            return f"{self.error_bound}"
        
    
    def create_output_file_absolute_path(self):
        return os.path.join(self.path_to_save, self.create_unique_file_name())


@dataclass
class UserInput:
    path_to_executable: str
    table_name: str
    error_bounds: List[float]
    path_to_table_data: str
    path_to_save: str
    path_to_metadata_database: str
    query_sample_size_per_interval: int
    writer_properties: List[ParquetWriterProperties]


def start_experiment(experiment_properties:ExperimentProperties, writer_properties: ParquetWriterProperties, logger: logging.Logger):
    tic = time.perf_counter()
    # Usage: ./target/release/low-level-writer table_name error_bound[add % for relative] /path/to/parquet_files /path/to/save unique_file_name /path/to/sqlite_db 
    # KEY=VALUE[ COMPRESSION=BROTLI COMPRESSION_LEVEL=1 STATISTICS_ENABLED=true or BLOOM_FILTER_ENABLED=true ]
    output_file_path = experiment_properties.create_output_file_absolute_path()
    try:
        tic = time.perf_counter()
        result = subprocess.run(
            [experiment_properties.executable_path, experiment_properties.table_name, experiment_properties.spell_out_error_bound(), experiment_properties.source_file_or_directory, experiment_properties.path_to_save, experiment_properties.create_unique_file_name(),  experiment_properties.metadata_database_path] + writer_properties.spell_for_terminal(),
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                text=True,
                check=True
            )

        logger.info(f"Data ingestion subprocess completed successfully in {time.perf_counter() - tic:.2f} seconds")
        logger.debug("Subprocess stdout:\n%s", result.stdout)

    except subprocess.CalledProcessError as exc:
        logger.error("Subprocess failed with return code %s", exc.returncode)
        logger.error("Command: %s", exc.cmd)
        logger.error("stderr:\n%s", exc.stderr)
        logger.error("stdout:\n%s", exc.stdout)
        return

    # Run queries
    # run only once
    tic = time.perf_counter()
    try:
        datafusion_read_page_index = False if experiment_properties.writer_properties.statistics_enabled == STATISTICS_ENABLED['none'] else True
        run_queries(
            writer_properties.max_row_group_size,
            experiment_properties.table_name,
            experiment_properties.create_unique_file_name(),
            output_file_path,
            experiment_properties.metadata_database_path,
            datafusion_read_page_index = datafusion_read_page_index,
        )

        logger.info(f"Completed all queries for: {output_file_path}")
        elapsed = time.perf_counter() - tic
        logger.info("Query Execution finished in %.2f seconds", elapsed)
    except Exception as e:
        logger.exception(f"The script failed in query execution phase")
        print(f"Error: {e}")
        sys.exit(-1)

    # remove the output file
    try:
        os.remove(output_file_path)
    except Exception as e:
        logger.exception("Exception could not remove the file")
        print(f"Error: {e}")
        sys.exit(-1)


def main(user_input: UserInput, logger: logging.Logger):
    # create a list of parameters and default values
    # iterate over parameter and error bound:
    logger.info("**************** EXPERIMENT START ****************")

    for writer_properties in user_input.writer_properties: # use ISOLATED_TEST_PARAMETERS for programmatic testing
        try:
            tic = time.perf_counter()
            # generate queries if they were not generated yet
            generate_queries(
                sample_size_per_interval = user_input.query_sample_size_per_interval,
                row_group_size = writer_properties.max_row_group_size,
                dataset_name = user_input.table_name,
                path_to_parquet_file_files = user_input.path_to_table_data, 
                database_path = user_input.path_to_metadata_database,
                logger = logger
            )
            query_generation_end = time.perf_counter()
            logger.info("Query generation finished in %.2f seconds", query_generation_end - tic)
            generate_query_execution_order(
                row_group_size = writer_properties.max_row_group_size,
                table_name = user_input.table_name,
                path_to_query_database= user_input.path_to_metadata_database,
                logger = logger
            )
            tac = time.perf_counter()
            logger.info("Query schedule generation finished in %.2f seconds", tac - query_generation_end)
            logger.info(f"Overall queries generated for row group: {writer_properties.max_row_group_size} in {tac-tic:.2f} seconds")

        except Exception as e:
            logger.exception("Exception could not generate queries or query schedule")
            print(f"Error: {e}")
            sys.exit(-1)

        for error_bound in user_input.error_bounds:
            # We need to create a path for the data to be ingested
            experiment_properties = ExperimentProperties(
                created_at=str(datetime.datetime.now()),
                table_name=user_input.table_name,
                source_file_or_directory=user_input.path_to_table_data,
                executable_path=user_input.path_to_executable,
                metadata_database_path=user_input.path_to_metadata_database,
                path_to_save=user_input.path_to_save,
                error_bound=error_bound,
                node_name=socket.gethostname(),
                commit_hash=get_git_commit_hash(),
                writer_properties=writer_properties
            )

            # 1. Dump each parameter to a json file
            with open("experiment_logs.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(experiment_properties)) + "\n")

            # 2. Run a rust program using parameters and
            # 3. Run queries using duckdb and data fusion three times and write the results into the sqlite database that rust program uses
            start_experiment(experiment_properties, writer_properties, logger)
    logger.info("**************** EXPERIMENT END ****************")


def _parse_manual_configurations(manual_configs: list) -> List[ParquetWriterProperties]:
    if not isinstance(manual_configs, list):
        print("'configurations.manual' must be a list")
        sys.exit(-1)

    if len(manual_configs) == 0:
        print("'configurations.manual' cannot be empty")
        sys.exit(-1)

    return [ParquetWriterProperties(**exp) for exp in manual_configs]


def _parse_isolated_configurations(isolated_config: dict) -> List[ParquetWriterProperties]:
    if not isinstance(isolated_config, dict):
        print("'configurations.isolated' must be a dictionary")
        sys.exit(-1)

    if len(isolated_config) == 0:
        print("'configurations.isolated' cannot be empty")
        sys.exit(-1)

    default = ParquetWriterProperties()
    experiments = []

    for param_name, param_values in isolated_config.items():
        if param_name == "compression":
            for compression_type, levels in param_values.items():
                if levels is None:
                    experiments.append(
                        replace(default, compression=COMPRESSION[compression_type])
                    )
                else:
                    for level in levels:
                        experiments.append(
                            replace(default, compression=COMPRESSION[compression_type], compression_level=level)
                        )
        else:
            if isinstance(param_values, list):
                values = param_values
            else:
                values = [param_values]

            for value in values:
                experiments.append(
                    replace(default, **{param_name: value})
                )

    return experiments


def parse_configurations(configuration_file: str, configuration_flag: ConfigFlag) -> UserInput:

    if not Path(configuration_file).is_file():
        print(f"{configuration_file} not found in the root")
        sys.exit(-1)

    required_experiment_setup_keys = {
        "path_to_executable",
        "table_name",
        "error_bound",
        "path_to_table_files",
        "path_to_save",
        "path_to_metadata_database",
        "query_sample_size_per_interval"
    }

    with open(configuration_file) as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        print("YAML root must be a dictionary")
        sys.exit(-1)

    for field in ['experimental_setup', 'configurations']:
        if field not in config.keys():
            print(f"{configuration_file} missing {field} field")
            sys.exit(-1)
    experiment_setup = config['experimental_setup']

    # Validate nested experiment_setup keys
    missing_experiment_keys = (
        required_experiment_setup_keys
        - experiment_setup.keys()
    )

    if missing_experiment_keys:
        print(
            f"Missing experiment_setup keys: "
            f"{sorted(missing_experiment_keys)}"
        )
        sys.exit(-1)

    # Parse configurations based on flag
    configurations = config["configurations"]

    if not isinstance(configurations, dict):
        print("'configurations' must be a dictionary")
        sys.exit(-1)

    if configuration_flag == ConfigFlag.DEFAULT:
        # returns only a list one element of default ParquetWriterProperties()
        writer_properties = [ ParquetWriterProperties() ] 
    elif configuration_flag == ConfigFlag.ISOLATED:
        if "isolated" not in configurations:
            print("'configurations.isolated' not found")
            sys.exit(-1)
        writer_properties = _parse_isolated_configurations(configurations["isolated"])
    elif configuration_flag == ConfigFlag.MANUAL:
        if "manual" not in configurations:
            print("'configurations.manual' not found")
            sys.exit(-1)
        writer_properties = _parse_manual_configurations(configurations["manual"])
    else:
        print(f"Unsupported configuration flag: {configuration_flag}")
        sys.exit(-1)

    metadata_database = os.path.abspath(config['experimental_setup']['path_to_metadata_database'])

    return UserInput (
        path_to_executable = config['experimental_setup']['path_to_executable'],
        table_name = config['experimental_setup']['table_name'],
        error_bounds = config['experimental_setup']['error_bound'],
        path_to_table_data = config['experimental_setup']['path_to_table_files'],
        path_to_save = config['experimental_setup']['path_to_save'],
        path_to_metadata_database = metadata_database,
        query_sample_size_per_interval = int(config['experimental_setup']['query_sample_size_per_interval']),
        writer_properties=writer_properties
    )


RUST_WRITER_DIR = Path(__file__).resolve().parent.parent / "writers" / "rust"


def build_rust_writer():
    """Run `cargo build --release` for the Rust writer so every run uses a binary built from the current source."""
    print(f"Building the Rust writer: cargo build --release (in {RUST_WRITER_DIR})", flush=True)
    try:
        subprocess.run(["cargo", "build", "--release"], cwd=RUST_WRITER_DIR, check=True)
    except FileNotFoundError:
        print("cargo not found. Install Rust (https://rustup.rs) and make sure cargo is on PATH.")
        sys.exit(-1)
    except subprocess.CalledProcessError as exc:
        print(f"cargo build --release failed with exit code {exc.returncode}")
        sys.exit(-1)


if __name__ == "__main__":
    build_rust_writer()

    yaml_file_name = "./configurations.yaml"

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} experiment_flag[ DEFAULT ISOLATED MANUAL ] ")
        sys.exit(-1)

    conf_flag_name = sys.argv[1]

    if conf_flag_name not in [enum.name for enum in ConfigFlag]:
        print(f"experiment_flag must be one of those: {[enum.name for enum in ConfigFlag]}")
        sys.exit(-1)

    configuration_flag = ConfigFlag[conf_flag_name]

    user_input = parse_configurations(yaml_file_name, configuration_flag)
       
    logging.basicConfig(
        filename="experiment.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger = logging.getLogger(__name__)
    tic = time.perf_counter()

    main(user_input, logger)
    
    logger.info(f"Experiment in {socket.gethostname()} with configuration flag: {configuration_flag.name} finished successfully in {time.perf_counter() - tic:.2f} seconds")
