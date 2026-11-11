use rusqlite::{Connection, Error as SqliteError, Result, params};
use std::collections::HashMap;
use std::fs::File;
use std::num::ParseIntError;
use std::sync::Arc;
use std::time::Duration;
use std::io;

use std::path::Path;
use std::path::PathBuf;

use thiserror::Error;

use arrow_schema::Schema;
use parquet::arrow::arrow_reader;
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::{DEFAULT_OFFSET_INDEX_DISABLED, EnabledStatistics};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::{ColumnPath, SchemaDescriptor};

use crate::macaquev::ErrorBound;
use crate::utils::{self, check_parquet_file_size};

pub const DEFAULT_FILE_NAME: &str = "output.parquet";
// Default values according to https://docs.rs/parquet/latest/parquet/file/properties/struct.WriterPropertiesBuilder.html#method.set_statistics_enabled
// Based on number of values
pub const DEFAULT_COMPRESSION: Compression = Compression::UNCOMPRESSED;
pub const DEFAULT_COMPRESSION_LEVEL: usize = 1;
pub const DEFAULT_ENCODING: Encoding = Encoding::PLAIN;
pub const WRITE_BATCH_SIZE: usize = 1024;
pub const READ_BATCH_SIZE: usize = 1024;
pub const DEFAULT_MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;
pub const STATISTICS_ENABLED: EnabledStatistics = EnabledStatistics::None;
// Based on number of bytes
pub const DEFAULT_DATA_PAGE_SIZE_LIMIT: usize = 1024 * 1024;
pub const BLOOM_FILTER_ENABLED: bool = false;
pub const DICTIONARY_ENABLED: bool = false;

pub struct UserInputDataset {
    pub dataset_name: String,
    pub str_error_bound: String,
    pub parquet_files_path: String,
    pub file_save_path: String,
    pub unique_file_name: String,
    pub statistics_save_path: String,
    pub configuration_map: HashMap<String, String>,
    pub parquet_files_list: Vec<PathBuf>,
}

impl UserInputDataset {
    /// Construct a new `UserInputDataset` from raw CLI arguments.
    ///
    /// All incoming references are converted to owned `String`s.
    pub fn new(
        dataset_name: String,
        str_error_bound: String,
        parquet_files_path: String,
        file_save_dir: String,
        unique_file_name: String,
        statistics_save_path: String,
        configuration_map: HashMap<String, String>,
    ) -> UserInputDataset {
        UserInputDataset {
            dataset_name: dataset_name,
            str_error_bound: str_error_bound,
            parquet_files_path: parquet_files_path.clone(),
            file_save_path: file_save_dir,
            unique_file_name: unique_file_name,
            statistics_save_path: statistics_save_path,
            configuration_map,
            parquet_files_list: utils::get_parquet_files(&parquet_files_path),
        }
    }
}

pub enum UsedDatasets {
    Parklog,
    Turbinelog,
    Engie,
    Blued,
    Redd,
    Voltagelog,
    ParklogTest,
    TickDataTest,
    M1DataTest,
    Wind,
    NeonWind,
    Greend,
}

#[derive(Debug, Error)]
pub enum WriteStatsError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] SqliteError),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    #[error("Missing execution time for file: {0}")]
    MissingExecutionTime(String),

    #[error("Failed to compute file size for: {0:?}")]
    FileSize(PathBuf),

    #[error("Page read error: {0}")]
    PageRead(String),

    #[error("Parse integer error: {0}")]
    ParseIntError(#[from] ParseIntError),
}

impl UsedDatasets {
    /// Return a lowercase string representation suitable for comparisons.
    pub fn as_str(&self) -> &'static str {
        match self {
            UsedDatasets::Parklog => "parklog",
            UsedDatasets::Turbinelog => "turbinelog",
            UsedDatasets::Engie => "engie",
            UsedDatasets::Blued => "blued",
            UsedDatasets::Redd => "redd",
            UsedDatasets::Voltagelog => "voltagelog",
            UsedDatasets::ParklogTest => "parklog_test",
            UsedDatasets::TickDataTest => "tickdata_test",
            UsedDatasets::M1DataTest => "m1data_test",
            UsedDatasets::Wind => "wind",
            UsedDatasets::NeonWind => "neon_wind",
            UsedDatasets::Greend => "greend",
        }
    }
}

impl std::fmt::Display for UsedDatasets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub struct Dataset {
    pub name: UsedDatasets,
    pub path_to_read: String,
    pub error_bound: ErrorBound,
    pub schema: SchemaDescriptor,
    pub arrow_schema: Arc<Schema>,
    pub file_save_path: String,
    pub unique_file_name: String,
    bloom_filter_enabled: bool,
    sorting_columns: Option<Vec<SortingColumn>>,
    data_page_size_limit: usize,
    dictionary_page_size_limit: usize,
    dictionary_enabled: bool,
    max_row_group_size: usize,
    // block compressor
    compression: Compression,
    compression_level: usize,
    // encoding
    timestamp_encoding: Encoding,
    value_encoding: Encoding,
    statistics_enabled: EnabledStatistics,
    offset_index_disabled: bool,
    // write_batch_size controls how many rows are processed per encoding step
    write_batch_size: usize,
    read_batch_size: usize,
}

impl Dataset {
    pub fn new(input_dataset: &UserInputDataset) -> Result<Dataset, WriteStatsError> {
        let mut dataset: Dataset = Dataset {
            name: parse_dataset_name(input_dataset),
            error_bound: parse_error_bound(input_dataset),
            path_to_read: input_dataset.parquet_files_path.clone(),
            file_save_path: input_dataset.file_save_path.clone(),
            schema: get_schema_descriptor(&input_dataset)?,
            arrow_schema: get_arrow_schema(&input_dataset)?,
            unique_file_name: input_dataset.unique_file_name.clone(),
            compression: DEFAULT_COMPRESSION,
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            bloom_filter_enabled: BLOOM_FILTER_ENABLED,
            sorting_columns: None,
            data_page_size_limit: DEFAULT_DATA_PAGE_SIZE_LIMIT,
            dictionary_page_size_limit: DEFAULT_DATA_PAGE_SIZE_LIMIT,
            dictionary_enabled: DICTIONARY_ENABLED,
            max_row_group_size: DEFAULT_MAX_ROW_GROUP_SIZE,
            timestamp_encoding: DEFAULT_ENCODING,
            value_encoding: DEFAULT_ENCODING,
            statistics_enabled: STATISTICS_ENABLED,
            offset_index_disabled: DEFAULT_OFFSET_INDEX_DISABLED,
            write_batch_size: WRITE_BATCH_SIZE,
            read_batch_size: READ_BATCH_SIZE,
        };

        // Set user specified parquet configurations
        for (config, value) in input_dataset.configuration_map.clone() {
            if config.eq_ignore_ascii_case("compression") {
                if input_dataset
                    .configuration_map
                    .contains_key("COMPRESSION_LEVEL")
                {
                    dataset.set_compression_level(
                        input_dataset
                            .configuration_map
                            .get("COMPRESSION_LEVEL")
                            .unwrap()
                            .parse()?,
                    );
                    dataset.set_compression(parse_parquet_compressor_string(
                        value,
                        Some(dataset.compression_level),
                    )?);
                } else {
                    // Compression level is not set so default compression level 1 is used
                    // if not compression_level is chosen than compression is set like ZSTD or BROTLI
                    dataset.set_compression(parse_parquet_compressor_string(value, None)?);
                }
            } else if config.eq_ignore_ascii_case("compression_level") {
                // we need to avoid this key since it is already used in the above branch
                continue;
            } else {
                dataset.set_configuration(config, value);
            }
        }

        Ok(dataset)
    }

    pub fn write_dataset_struct_parameters_to_sqlite(
        &self,
        sqlite_path: &str,
        table_name: &str,
        execution_time_map: &HashMap<String, Duration>,
        file_execution_time: &Duration,
    ) -> Result<(), WriteStatsError> {
        // Open or create SQLite database
        let db_path = Path::new(sqlite_path);
        let conn = Connection::open(db_path)?;
        // Create table if missing
        let create_sql = format!(
            r#"
	            CREATE TABLE IF NOT EXISTS {} (
                    dataset_name TEXT,
                    output_file_name TEXT,
                    file_read_dir TEXT,
                    file_save_dir TEXT, 
                    file_absolute_path TEXT,
                    error_bound TEXT,
                    relative_error_bound INTEGER,
                    value_encoding TEXT,
                    timestamp_encoding TEXT,
                    compression TEXT,
                    compression_level INTEGER,
                    file_size_in_bytes INTEGER,
                    compression_time_in_secs REAL,
                    max_number_of_rows_in_row_group INTEGER,
                    write_batch_size INTEGER,
                    read_batch_size INTEGER,
                    data_page_size_limit INTEGER,
                    dictionary_page_size_limit INTEGER,
                    bloom_filter_enabled INTEGER,
                    sorting_columns TEXT, 
                    dictionary_enabled INTEGER, 
                    statistics_enabled TEXT,
                    offset_index_disabled INTEGER
                ) STRICT;
                "#,
            table_name
        );

        conn.execute(&create_sql, [])?;

        // Convert sorting columns to printable string
        let sorting_columns_string = match &self.sorting_columns {
            Some(cols) => {
                let names: Vec<String> = cols.iter().map(|c| format!("{:?}", c)).collect();
                names.join(";")
            }
            None => "".to_string(),
        };
        let file_absolute_path = self.get_output_file_absolute_path();
        let file_size_in_bytes = check_parquet_file_size(&file_absolute_path);
        // It could be a hashmap that stores the timer for multiple parquet writers or a single duration object
        let file_execution_time = if execution_time_map.is_empty() {
            file_execution_time
        } else {
            execution_time_map
                .get(&self.unique_file_name)
                .ok_or_else(|| {
                    WriteStatsError::MissingExecutionTime(self.unique_file_name.clone())
                })?
        };

        // Prepare INSERT statement
        let insert_sql = format!(
            r#"
                INSERT INTO {} 
                VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                );
                "#,
            table_name
        );

        let mut stmt = conn.prepare(&insert_sql)?;
        // insert row exactly matching the CSV order
        stmt.execute(params![
            self.name.to_string(),
            self.unique_file_name,
            self.path_to_read,
            self.file_save_path,
            file_absolute_path.to_str(),
            self.error_bound.to_string(),
            self.error_bound.is_relative(),
            self.value_encoding.to_string(),
            self.timestamp_encoding.to_string(),
            self.compression.to_string(),
            self.get_default_compression_level(),
            file_size_in_bytes,
            file_execution_time.as_secs(),
            self.max_row_group_size as i64,
            self.write_batch_size as i64,
            self.read_batch_size as i64,
            self.data_page_size_limit as i64,
            self.dictionary_page_size_limit as i64,
            if self.bloom_filter_enabled { 1 } else { 0 },
            sorting_columns_string,
            if self.dictionary_enabled { 1 } else { 0 },
            self.get_statistics_enabled_str(),
            if self.offset_index_disabled { 1 } else { 0 },
        ])?;

        println!(
            "✓ Inserted writer parameters for dataset '{}' into {}",
            self.name, sqlite_path
        );
        Ok(())
    }

    pub fn write_parquet_file_statistics_to_sqlite(
        &self,
        sqlite_path: &str,
        file_metadata_table_name: &str,
        execution_time_map: &HashMap<String, Duration>,
        execution_timer: &Duration,
    ) -> Result<(), WriteStatsError> {
        // ---- Open SQLite database ----
        let db_path = Path::new(sqlite_path);
        let conn = Connection::open(db_path)?;

        // ---- CREATE TABLE IF NOT EXISTS ----
        let create_sql = format!(
            r#"
        CREATE TABLE IF NOT EXISTS {} (
            dataset_name TEXT,
            output_file_name TEXT,
            file_absolute_path TEXT,
            error_bound TEXT,
            created_by TEXT,
            file_size_in_bytes INTEGER,
            compression_time_in_secs REAL,
            number_of_row_groups INTEGER,
            row_group_index INTEGER,
            number_of_rows_in_row_group INTEGER,
            row_group_total_byte_size INTEGER, 
            column_index INTEGER,
            column_name TEXT,
            column_data_type TEXT,
            column_default_compression TEXT,
            column_encoding TEXT,
            column_number_of_values INTEGER,
            column_compressed_size INTEGER,
            column_uncompressed_size INTEGER,
            statistics_enabled_in_column INTEGER,
            stats_null_count_in_column INTEGER,
            stats_distinct_count_in_column INTEGER,
            total_page_number_of_values_in_column INTEGER,
            total_pages_in_column INTEGER,
            total_page_size_in_bytes_in_column INTEGER,
            number_of_data_pages_in_column INTEGER,
            data_pages_number_of_values_in_column INTEGER,
            number_of_dict_pages_in_column INTEGER,
            index_pages_number_of_values_in_column INTEGER,
            number_of_index_pages_in_column INTEGER,
            dict_pages_number_of_values_in_column INTEGER,
            number_of_data_v2_pages_in_column INTEGER,
            data_v2_pages_number_of_values_in_column INTEGER,
            bloom_filter_enabled_in_column INTEGER,
            dictionary_enabled_in_column INTEGER,
            sorting_columns TEXT
        ) STRICT;
        "#,
            file_metadata_table_name
        );

        conn.execute(&create_sql, [])?;

        // ---- Open Parquet file ----
        let file_absolute_path = self.get_output_file_absolute_path();
        let file = File::open(&file_absolute_path)?;
        let file_size_in_bytes = check_parquet_file_size(&file_absolute_path);

        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let file_meta = metadata.file_metadata();

        let dataset_name = self.name.to_string();
        let error_bound = self.error_bound.to_string();
        let created_by = file_meta.created_by().unwrap_or("");
        let number_of_row_groups = metadata.num_row_groups();

        // ---- Determine execution time ----
        let file_execution_time = if execution_time_map.is_empty() {
            execution_timer
        } else {
            execution_time_map
                .get(&self.unique_file_name)
                .ok_or_else(|| {
                    WriteStatsError::MissingExecutionTime(self.unique_file_name.clone())
                })?
        };

        // ---- Prepare INSERT statement ----
        let insert_sql = format!(
            r#"
        INSERT INTO {} 
        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        );
        "#,
            file_metadata_table_name
        );

        let mut insert_stmt = conn.prepare(&insert_sql)?;

        // TODO: Sorting columns are NOT supported by the Apache Arrow parquet crate in Rust
        let sorting_columns = "not_available".to_string();

        //Iterate row groups + columns
        for row_group_index in 0..number_of_row_groups {
            let row_group_metadata = metadata.row_group(row_group_index);
            let row_group_reader = reader.get_row_group(row_group_index)?;

            let row_group_number_of_rows = row_group_metadata.num_rows();
            let row_group_total_byte_size = row_group_metadata.total_byte_size();

            for column_index in 0..row_group_metadata.num_columns() {
                let column_metadata = row_group_metadata.column(column_index);
                column_metadata.compressed_size();
                let column_path = column_metadata.column_path().string();
                let column_data_type = format!("{:?}", column_metadata.column_type());
                let column_block_compressor = format!("{:?}", column_metadata.compression());
                let encodings_vec: Vec<String> = column_metadata
                    .encodings()
                    .map(|e| format!("{:?}", e))
                    .collect();
                let column_encoding = encodings_vec.join(";");

                let column_number_of_values = column_metadata.num_values();
                let column_compressed_size = column_metadata.compressed_size();
                let column_uncompressed_size = column_metadata.uncompressed_size();

                // Statistics
                let (statistics_enabled, stats_null_count, stats_distinct_count, _, _) =
                    if let Some(stats) = column_metadata.statistics() {
                        (
                            true,
                            stats.null_count_opt().unwrap_or(0),
                            stats.distinct_count_opt().unwrap_or(0),
                            stats.min_bytes_opt(),
                            stats.max_bytes_opt(),
                        )
                    } else {
                        (false, 0, 0, None, None)
                    };

                // 1) Bloom filter enabled
                let bloom_filter_enabled = column_metadata.bloom_filter_offset().is_some()
                    || column_metadata.bloom_filter_length().is_some();

                // 2) Dictionary enabled
                let dictionary_enabled = column_metadata
                    .encodings()
                    .any(|e| format!("{:?}", e).contains("RLE_DICTIONARY"));

                // ---- Page iteration ----
                let mut page_reader = row_group_reader.get_column_page_reader(column_index)?;
                let mut total_pages_in_column: i32 = 0;
                let mut total_page_number_of_values_in_column: i64 = 0;
                let mut total_page_size_in_bytes = 0;

                let mut number_of_data_pages_in_column = 0;
                let mut data_pages_number_of_values_in_column = 0;

                let mut number_of_index_pages_in_column = 0;
                let mut index_pages_number_of_values_in_column = 0;

                let mut number_of_dict_pages_in_column = 0;
                let mut dict_pages_number_of_values_in_column = 0;

                let mut number_of_data_v2_pages_in_column = 0;
                let mut data_v2_pages_number_of_values_in_column = 0;

                while let Some(page_result) = page_reader.next() {
                    let page = page_result.map_err(|e| WriteStatsError::PageRead(e.to_string()))?;

                    total_pages_in_column += 1;
                    total_page_number_of_values_in_column += page.num_values() as i64;
                    total_page_size_in_bytes += page.buffer().len();
                    match page.page_type() {
                        parquet::basic::PageType::DATA_PAGE => {
                            number_of_data_pages_in_column += 1;
                            data_pages_number_of_values_in_column += page.num_values();
                        }
                        parquet::basic::PageType::INDEX_PAGE => {
                            number_of_index_pages_in_column += 1;
                            index_pages_number_of_values_in_column += page.num_values();
                        }
                        parquet::basic::PageType::DICTIONARY_PAGE => {
                            number_of_dict_pages_in_column += 1;
                            dict_pages_number_of_values_in_column += page.num_values();
                        }
                        parquet::basic::PageType::DATA_PAGE_V2 => {
                            number_of_data_v2_pages_in_column += 1;
                            data_v2_pages_number_of_values_in_column += page.num_values();
                        }
                    }
                }

                // ---- INSERT ----
                insert_stmt.execute(params![
                    dataset_name,
                    self.unique_file_name,
                    file_absolute_path.to_string_lossy(),
                    error_bound,
                    created_by,
                    file_size_in_bytes,
                    file_execution_time.as_secs_f64(),
                    number_of_row_groups as i64,
                    row_group_index as i64,
                    row_group_number_of_rows as i64,
                    row_group_total_byte_size as i64,
                    column_index as i64,
                    column_path,
                    column_data_type,
                    column_block_compressor,
                    column_encoding,
                    column_number_of_values as i64,
                    column_compressed_size as i64,
                    column_uncompressed_size as i64,
                    statistics_enabled as i64,
                    stats_null_count as i64,
                    stats_distinct_count as i64,
                    total_page_number_of_values_in_column,
                    total_pages_in_column,
                    total_page_size_in_bytes,
                    number_of_data_pages_in_column,
                    data_pages_number_of_values_in_column,
                    number_of_dict_pages_in_column,
                    index_pages_number_of_values_in_column,
                    number_of_index_pages_in_column,
                    dict_pages_number_of_values_in_column,
                    number_of_data_v2_pages_in_column,
                    data_v2_pages_number_of_values_in_column,
                    bloom_filter_enabled as i64,
                    dictionary_enabled as i64,
                    sorting_columns, // not available for now
                ])?;
            }
        }

        println!(
            "✓ Inserted file properties for dataset '{}' into {}",
            self.name, sqlite_path
        );

        Ok(())
    }

    pub fn write_parquet_writer_parameters_to_sqlite(
        &self,
        sqlite_database_name: &str,
        writer_properties_table_name: &str,
        properties: &Arc<WriterProperties>,
    ) -> rusqlite::Result<(), WriteStatsError> {
        let conn = Connection::open(sqlite_database_name)?;

        // Create table if it does not exist
        conn.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {} (
                        output_file_name          TEXT,
                        column_name               TEXT,
                        bloom_filter_enabled      INTEGER,
                        compression               TEXT,
                        dictionary_enabled        INTEGER,
                        max_row_group_size        INTEGER,
                        data_page_size_limit      INTEGER,
                        write_batch_size          INTEGER,
                        statistics_enabled        TEXT,
                        encoding                  TEXT,
                        writer_version            TEXT,
                        created_by                TEXT,
                        sorting_columns           TEXT
                    ) STRICT",
                writer_properties_table_name
            ),
            [],
        )?;
        let column_paths = collect_column_paths(&self.schema);

        for column_path in column_paths {
            let column_name = column_path.to_string();
            // Extract values explicitly
            let bloom_filter_enabled = if properties.bloom_filter_properties(&column_path).is_some()
            {
                1
            } else {
                0
            };
            let dictionary_enabled = properties.dictionary_enabled(&column_path) as i32;
            let compression = format!("{:?}", properties.compression(&column_path));
            let max_row_group_size = properties.max_row_group_row_count().unwrap_or(0) as i64;
            let data_page_size_limit = properties.data_page_size_limit() as i64;
            let write_batch_size = properties.write_batch_size() as i64;
            let statistics_enabled = format!("{:?}", properties.statistics_enabled(&column_path));
            let encoding = format!("{:?}", properties.encoding(&column_path));
            let writer_version = format!("{:?}", properties.writer_version());
            let created_by = properties.created_by();
            let sorting_columns = "None"; // TODO: to implement this
            // Prepare INSERT statement
            let insert_sql = format!(
                r#"
                    INSERT INTO {} 
                    VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?
                    );
                "#,
                writer_properties_table_name
            ); // Insert one row

            let mut stmt = conn.prepare(&insert_sql)?;

            stmt.execute(params![
                self.unique_file_name,
                column_name,
                bloom_filter_enabled,
                compression,
                dictionary_enabled,
                max_row_group_size,
                data_page_size_limit,
                write_batch_size,
                statistics_enabled,
                encoding,
                writer_version,
                created_by,
                sorting_columns
            ])?;
        }

        _ = conn.close();
        Ok(())
    }

    pub fn create_parquet_writer_properties_batch(
        &self,
        encoding: Encoding,
        dictionary_enabled: bool,
    ) -> WriterProperties {
        match self.name {
            UsedDatasets::Blued => {
                WriterProperties::builder()
                    .set_data_page_size_limit(self.data_page_size_limit)
                    .set_max_row_group_row_count(Some(self.max_row_group_size))
                    .set_bloom_filter_enabled(self.bloom_filter_enabled)
                    .set_dictionary_enabled(dictionary_enabled)
                    .set_sorting_columns(self.sorting_columns.clone())
                    .set_statistics_enabled(self.statistics_enabled)
                    .set_write_batch_size(self.write_batch_size)
                    .set_offset_index_disabled(self.offset_index_disabled)
                    // set column encoding
                    .set_column_encoding(ColumnPath::from("time"), encoding)
                    .set_column_encoding(ColumnPath::from("current_a"), encoding)
                    .set_column_encoding(ColumnPath::from("current_b"), encoding)
                    .set_column_encoding(ColumnPath::from("voltage"), encoding)
                    // set column compression
                    .set_compression(self.compression)
                    .set_column_compression(ColumnPath::from("voltage"), self.compression)
                    .set_column_compression(ColumnPath::from("current_a"), self.compression)
                    .set_column_compression(ColumnPath::from("current_b"), self.compression)
                    .set_column_compression(ColumnPath::from("voltage"), self.compression)
                    .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                    .build()
            }
            UsedDatasets::Parklog => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), encoding)
                .set_column_encoding(ColumnPath::from("ActivePower"), encoding)
                .set_column_encoding(ColumnPath::from("ActivePower60"), encoding)
                .set_column_encoding(ColumnPath::from("ActivePower600"), encoding)
                .set_column_encoding(ColumnPath::from("RawReactivePower"), encoding)
                .set_column_encoding(ColumnPath::from("RawPower"), encoding)
                .set_column_encoding(ColumnPath::from("PowerError"), encoding)
                .set_column_encoding(ColumnPath::from("PowerUpperLimit"), encoding)
                .set_column_encoding(ColumnPath::from("PowerLowerLimit"), encoding)
                .set_column_encoding(ColumnPath::from("AvailablePower"), encoding)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Turbinelog => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), encoding)
                .set_column_encoding(ColumnPath::from("ReactivePower"), encoding)
                .set_column_encoding(ColumnPath::from("ActivePower"), encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), encoding)
                .set_column_encoding(ColumnPath::from("PowerReference"), encoding)
                .set_column_encoding(ColumnPath::from("AssignPowerRef"), encoding)
                .set_column_encoding(ColumnPath::from("AvailablePower"), encoding)
                .set_column_encoding(ColumnPath::from("AvailablePowerFiltered"), encoding)
                .set_column_encoding(ColumnPath::from("AvailablePowerRaw"), encoding)
                .set_column_dictionary_enabled(ColumnPath::from("Turbine"), true)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Wind => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("datetime"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("wind_speed"), encoding)
                .set_column_encoding(ColumnPath::from("pitch_angle"), encoding)
                .set_column_encoding(ColumnPath::from("rotor_speed"), encoding)
                .set_column_encoding(ColumnPath::from("active_power"), encoding)
                .set_column_encoding(ColumnPath::from("cos_nacelle_dir"), encoding)
                .set_column_encoding(ColumnPath::from("sin_nacelle_dir"), encoding)
                .set_column_encoding(ColumnPath::from("cos_wind_dir"), encoding)
                .set_column_encoding(ColumnPath::from("sin_wind_dir"), encoding)
                .set_column_encoding(ColumnPath::from("cor_nacelle_direction"), encoding)
                .set_column_encoding(ColumnPath::from("cor_wind_direction"), encoding)
                .set_compression(self.compression)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_dictionary_enabled(dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Redd => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("time"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("waveform_values_amps"), encoding)
                .set_compression(self.compression)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_dictionary_enabled(dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Voltagelog => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("ReactivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("Voltage"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("VoltageReference"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("VoltageDroop"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("Error"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("QMinMax"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("QMin"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("QMax"), self.value_encoding)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::ParklogTest => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), encoding)
                .set_column_encoding(ColumnPath::from("ActivePower"), encoding)
                .set_column_encoding(ColumnPath::from("ActivePower60"), encoding)
                .set_column_encoding(ColumnPath::from("ActivePower600"), encoding)
                .set_column_encoding(ColumnPath::from("RawReactivePower"), encoding)
                .set_column_encoding(ColumnPath::from("RawPower"), encoding)
                .set_column_encoding(ColumnPath::from("PowerError"), encoding)
                .set_column_encoding(ColumnPath::from("PowerUpperLimit"), encoding)
                .set_column_encoding(ColumnPath::from("PowerLowerLimit"), encoding)
                .set_column_encoding(ColumnPath::from("AvailablePower"), encoding)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(), 
            UsedDatasets::TickDataTest => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("DateTime"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Bid"), encoding)
                .set_column_encoding(ColumnPath::from("Ask"), encoding)
                .set_column_encoding(ColumnPath::from("Volume"), encoding)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::M1DataTest => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("DateTime"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Open"), encoding)
                .set_column_encoding(ColumnPath::from("High"), encoding)
                .set_column_encoding(ColumnPath::from("Low"), encoding)
                .set_column_encoding(ColumnPath::from("Close"), encoding)
                .set_column_encoding(ColumnPath::from("Volume"), encoding)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            _ => WriterProperties::builder().build()
            }
    }

    pub fn create_parquet_writer_properties_single_file(&self) -> Arc<WriterProperties> {
        let writer_properties = match self.name {
            UsedDatasets::Blued => {
                WriterProperties::builder()
                    .set_data_page_size_limit(self.data_page_size_limit)
                    .set_max_row_group_row_count(Some(self.max_row_group_size))
                    .set_bloom_filter_enabled(self.bloom_filter_enabled)
                    .set_dictionary_enabled(self.dictionary_enabled)
                    .set_sorting_columns(self.sorting_columns.clone())
                    .set_statistics_enabled(self.statistics_enabled)
                    .set_write_batch_size(self.write_batch_size)
                    .set_offset_index_disabled(self.offset_index_disabled)
                    // set column encoding
                    .set_column_encoding(ColumnPath::from("time"), self.timestamp_encoding)
                    .set_column_encoding(ColumnPath::from("current_a"), self.value_encoding)
                    .set_column_encoding(ColumnPath::from("current_b"), self.value_encoding)
                    .set_column_encoding(ColumnPath::from("voltage"), self.value_encoding)
                    // set column compression
                    .set_compression(self.compression)
                    .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                    .build()
            }
            UsedDatasets::Parklog => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ActivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ActivePower60"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ActivePower600"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("RawReactivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("RawPower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("PowerError"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("PowerUpperLimit"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("PowerLowerLimit"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("AvailablePower"), self.value_encoding)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Turbinelog => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ReactivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ActivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("PowerReference"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("AssignPowerRef"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("AvailablePower"), self.value_encoding)
                .set_column_encoding(
                    ColumnPath::from("AvailablePowerFiltered"),
                    self.value_encoding,
                )
                .set_column_encoding(ColumnPath::from("AvailablePowerRaw"), self.value_encoding)
                .set_column_dictionary_enabled(ColumnPath::from("Turbine"), true)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Wind => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("datetime"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("wind_speed"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("pitch_angle"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("rotor_speed"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("active_power"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("cos_nacelle_dir"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("sin_nacelle_dir"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("cos_wind_dir"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("sin_wind_dir"), self.value_encoding)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .set_column_encoding(
                    ColumnPath::from("cor_nacelle_direction"),
                    self.value_encoding,
                )
                .set_column_encoding(ColumnPath::from("cor_wind_direction"), self.value_encoding)
                .set_compression(self.compression)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Redd => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("time"), self.timestamp_encoding)
                .set_column_encoding(
                    ColumnPath::from("waveform_values_amps"),
                    self.value_encoding,
                )
                .set_column_dictionary_enabled(ColumnPath::from("device"), true)
                .set_compression(self.compression)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_dictionary_enabled(self.get_dictionary_enabled())
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Voltagelog => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("ReactivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("Voltage"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("VoltageReference"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("VoltageDroop"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("Error"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("QMinMax"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("QMin"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("QMax"), self.value_encoding)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::ParklogTest => WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("TimeStamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("Frequency"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ActivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ActivePower60"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("ActivePower600"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("RawReactivePower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("RawPower"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("PowerError"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("PowerUpperLimit"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("PowerLowerLimit"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("AvailablePower"), self.value_encoding)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::NeonWind =>  WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("endDateTime"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("windSpeedMean"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("windSpeedMinimum"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("windSpeedMaximum"), self.value_encoding)
                .set_column_dictionary_enabled(ColumnPath::from("device"), true)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            UsedDatasets::Greend =>  WriterProperties::builder()
                .set_column_encoding(ColumnPath::from("timestamp"), self.timestamp_encoding)
                .set_column_encoding(ColumnPath::from("coffee_machine"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("washing_machine"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("radio"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("water_kettle"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("fridge"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("dishwasher"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("lamp"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("tv"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("tv2"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("vacuum_cleaner"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("microwave"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("hairdryer"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("kitchenware"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("nas"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("tumble_dryer"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("computer"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("bread_machine"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("outlet"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("total_outlets"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("total_lights"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("electric_oven"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("hood"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("toaster"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("stove"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("iron"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("total_floor"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("total_floor2"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("total_floor3"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("modem"), self.value_encoding)
                .set_column_encoding(ColumnPath::from("freezer"), self.value_encoding)
                .set_compression(self.compression)
                .set_max_row_group_row_count(Some(self.max_row_group_size))
                .set_bloom_filter_enabled(self.bloom_filter_enabled)
                .set_column_dictionary_enabled(ColumnPath::from("building_id"), true)
                .set_data_page_size_limit(self.data_page_size_limit)
                .set_dictionary_enabled(self.dictionary_enabled)
                .set_sorting_columns(self.sorting_columns.clone())
                .set_statistics_enabled(self.statistics_enabled)
                .set_write_batch_size(self.write_batch_size)
                .set_offset_index_disabled(self.offset_index_disabled)
                .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
                .build(),
            // Since we are not using this method, we wont support the rest of the datasets
            _ => WriterProperties::builder().build()
        };
        Arc::new(writer_properties)
    }

    pub fn set_configuration(&mut self, configuration: String, key: String) {
        match configuration.as_str() {
            "BLOOM_FILTER_ENABLED" => {
                if key.eq_ignore_ascii_case("true") {
                    self.set_bloom_filter_enabled(true)
                } else if key.eq_ignore_ascii_case("false") {
                    self.set_bloom_filter_enabled(false)
                } else {
                    panic!("Parameter can be either true/false for BLOOM_FILTER_ENABLED")
                };
            }
            "SORTING_COLUMNS" => {
                // Accept special value "NONE" to clear sorting columns; otherwise leave as-is.
                if key.eq_ignore_ascii_case("NONE") {
                    self.set_sorting_columns(None);
                }
                // TODO: discuss if sorting columns is needed for time series since data is sorted by time
            }
            "DATA_PAGE_SIZE_LIMIT" => {
                if let Ok(v) = key.parse::<usize>() {
                    self.set_data_page_size_limit(v);
                }
            }
            "DICTIONARY_PAGE_SIZE_LIMIT" => {
                if let Ok(v) = key.parse::<usize>() {
                    self.set_dictionary_page_size_limit(v);
                }
            }
            "DICTIONARY_ENABLED" => {
                if key.eq_ignore_ascii_case("true") {
                    self.set_dictionary_enabled(true)
                } else if key.eq_ignore_ascii_case("false") {
                    return;
                } else {
                    panic!("Parameter can be either true/false for DICTIONARY_ENABLED")
                };
            }
            "MAX_ROW_GROUP_SIZE" => {
                if let Ok(v) = key.parse::<usize>() {
                    self.set_max_row_group_size(v);
                }
            }
            // ENCODINGS
            "TIMESTAMP_ENCODING" => {
                let enc = if key.eq_ignore_ascii_case("DELTA_BINARY_PACKED") {
                    Encoding::DELTA_BINARY_PACKED
                } else {
                    Encoding::PLAIN
                };
                self.set_timestamp_encoding(enc);
            }
            "VALUE_ENCODING" => {
                // RLE_DICTIONARY is enabled by through column_dict_encoding_enabled parameter
                let enc = if key.eq_ignore_ascii_case("BYTE_STREAM_SPLIT") {
                    Encoding::BYTE_STREAM_SPLIT
                } else {
                    Encoding::PLAIN
                };
                self.set_value_encoding(enc);
            }
            "STATISTICS_ENABLED" => {
                // only support disabling statistics explicitly; enabling is left to defaults
                if key.eq_ignore_ascii_case("none") {
                    self.set_statistics_enabled(EnabledStatistics::None);
                } else if key.eq_ignore_ascii_case("chunk") {
                    self.set_statistics_enabled(EnabledStatistics::Chunk);
                } else if key.eq_ignore_ascii_case("page") {
                    self.set_statistics_enabled(EnabledStatistics::Page);
                } else {
                    panic!(
                        "Incorrect value for STATISTICS_ENABLED. Can be one of these: NONE CHUNK PAGE"
                    )
                }
            }
            "OFFSET_INDEX_DISABLED" => {
                if key.eq_ignore_ascii_case("true") {
                    self.set_offset_index_disabled(true);
                } else if key.eq_ignore_ascii_case("false") {
                    self.set_offset_index_disabled(false);
                } else {
                    panic!(
                        "Wrong key supplied for OFFSET_INDEX_DISABLED. Can be either true or false."
                    )
                };
            }
            "WRITE_BATCH_SIZE" => {
                if let Ok(v) = key.parse::<usize>() {
                    self.set_write_batch_size(v);
                }
            }
            "READ_BATCH_SIZE" => {
                if let Ok(v) = key.parse::<usize>() {
                    self.set_read_batch_size(v);
                }
            }
            _ => panic!("CONFIGURATION=KEY does not have a valid value!"),
        }
    }
    // Setters for configurable fields
    pub fn set_bloom_filter_enabled(&mut self, enabled: bool) {
        self.bloom_filter_enabled = enabled;
    }

    pub fn set_compression(&mut self, compression: Compression) {
        self.compression = compression;
    }

    pub fn set_compression_level(&mut self, level: usize) {
        self.compression_level = level;
    }

    pub fn set_sorting_columns(&mut self, cols: Option<Vec<SortingColumn>>) {
        self.sorting_columns = cols;
    }

    pub fn set_data_page_size_limit(&mut self, limit: usize) {
        self.data_page_size_limit = limit;
    }

    pub fn set_dictionary_page_size_limit(&mut self, limit: usize) {
        self.dictionary_page_size_limit = limit;
    }

    pub fn set_dictionary_enabled(&mut self, enabled: bool) {
        self.dictionary_enabled = enabled;
    }

    pub fn set_max_row_group_size(&mut self, size: usize) {
        self.max_row_group_size = size;
    }

    pub fn set_timestamp_encoding(&mut self, encoding: Encoding) {
        self.timestamp_encoding = encoding;
    }

    pub fn set_value_encoding(&mut self, encoding: Encoding) {
        self.value_encoding = encoding;
    }

    pub fn set_statistics_enabled(&mut self, stats: EnabledStatistics) {
        self.statistics_enabled = stats;
    }

    pub fn set_offset_index_disabled(&mut self, disabled: bool) {
        self.offset_index_disabled = disabled;
    }

    pub fn set_read_batch_size(&mut self, read_batch_size: usize) {
        self.read_batch_size = read_batch_size;
    }

    pub fn set_write_batch_size(&mut self, write_batch_size: usize) {
        self.write_batch_size = write_batch_size;
    }

    pub fn get_output_file_absolute_path(&self) -> PathBuf {
        Path::new(&self.file_save_path)
            .join(&self.unique_file_name)
            .to_path_buf()
    }

    // Getters for private fields
    pub fn get_bloom_filter_enabled(&self) -> bool {
        self.bloom_filter_enabled
    }

    pub fn get_sorting_columns(&self) -> &Option<Vec<SortingColumn>> {
        &self.sorting_columns
    }

    pub fn get_data_page_size_limit(&self) -> usize {
        self.data_page_size_limit
    }

    pub fn get_dictionary_enabled(&self) -> bool {
        self.dictionary_enabled
    }

    pub fn get_max_row_group_size(&self) -> usize {
        self.max_row_group_size
    }

    pub fn get_default_compression(&self) -> &Compression {
        &self.compression
    }

    pub fn get_default_compression_level(&self) -> usize {
        self.compression_level
    }

    pub fn get_timestamp_encoding(&self) -> &Encoding {
        &self.timestamp_encoding
    }

    pub fn get_statistics_enabled(&self) -> &EnabledStatistics {
        &self.statistics_enabled
    }

    pub fn get_statistics_enabled_str(&self) -> &'static str {
        match *self.get_statistics_enabled() {
            EnabledStatistics::None => "None",
            EnabledStatistics::Chunk => "Chunk",
            EnabledStatistics::Page => "Page",
        }
    }

    pub fn get_offset_index_disabled(&self) -> bool {
        self.offset_index_disabled
    }

    pub fn get_write_batch_size(&self) -> usize {
        self.write_batch_size
    }

    pub fn get_read_batch_size(&self) -> usize {
        self.read_batch_size
    }
}

fn get_schema_descriptor(
    input_dataset: &UserInputDataset,
) -> Result<SchemaDescriptor, WriteStatsError> {
    // Set schema and column related information
    // READ SCHEMA FROM FIRST FILE
    let input = File::open(&input_dataset.parquet_files_list[0])?;
    let reader = SerializedFileReader::new(input)?;
    Ok(reader.metadata().file_metadata().schema_descr().clone())
}

fn get_arrow_schema(input_dataset: &UserInputDataset) -> Result<Arc<Schema>, WriteStatsError> {
    let file = File::open(input_dataset.parquet_files_list.get(0).unwrap())?;
    // let mut last_values_map = create_last_value_map(builder.parquet_schema().num_columns());
    let builder = arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.schema().clone())
}

fn parse_error_bound(input_dataset: &UserInputDataset) -> ErrorBound {
    if input_dataset.str_error_bound.contains("%") {
        let parsed_error_bound: f32 = input_dataset
            .str_error_bound
            .strip_suffix("%")
            .unwrap()
            .parse()
            .unwrap();
        if parsed_error_bound == 0.0 {
            ErrorBound::Lossless
        } else {
            ErrorBound::Relative(parsed_error_bound)
        }
    } else {
        let parsed_error_bound: f32 = input_dataset.str_error_bound.parse().unwrap();
        if parsed_error_bound == 0.0 {
            ErrorBound::Lossless
        } else {
            ErrorBound::Absolute(parsed_error_bound)
        }
    }
}

fn parse_schema(input_dataset: &UserInputDataset) -> Arc<Schema> {
    let parquet_files = utils::get_parquet_files(&input_dataset.parquet_files_path);
    let file =
        File::open(&parquet_files[0]).expect("Files could not be read for reading initial schema");
    let builder = arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema: Arc<arrow_schema::Schema> = builder.schema().clone();
    schema
}

// helper for converting the dataset name string provided by the user into the
// `UsedDatasets` enum.  Comparison is case‑insensitive; the input is lowercased
// before matching.  Unknown names panic (the caller should validate earlier if
// desired).
fn parse_dataset_name(input_dataset: &UserInputDataset) -> UsedDatasets {
    match input_dataset.dataset_name.to_lowercase().as_str() {
        "parklog" => UsedDatasets::Parklog,
        "turbinelog" => UsedDatasets::Turbinelog,
        "engie" => UsedDatasets::Engie,
        "blued" => UsedDatasets::Blued,
        "redd" => UsedDatasets::Redd,
        "voltagelog" => UsedDatasets::Voltagelog,
        "parklog_test" => UsedDatasets::ParklogTest,
        "tickdata_test" => UsedDatasets::TickDataTest,
        "m1data_test" => UsedDatasets::M1DataTest,
        "wind" => UsedDatasets::Wind,
        "neon_wind" => UsedDatasets::NeonWind,
        "greend" => UsedDatasets::Greend,
        other => panic!(
            "unrecognized dataset name '{}', expected one of parklog, turbinelog, engie, blued, redd, parklog_test, tickdata_test, m1data_test",
            other
        ),
    }
}

// method for parsing the compressor string
fn parse_parquet_compressor_string(
    compression_string: String,
    compression_level: Option<usize>,
) -> Result<Compression, WriteStatsError> {
    let compression_level = match compression_level {
        Some(level) => level,
        _ => 1,
    };

    match compression_string.to_ascii_lowercase().as_str() {
        "brotli" => Ok(Compression::BROTLI(BrotliLevel::try_new(
            compression_level as u32,
        )?)),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::try_new(
            compression_level as i32,
        )?)),
        "gzip" => Ok(Compression::GZIP(GzipLevel::try_new(
            compression_level as u32,
        )?)),
        "snappy" => Ok(Compression::SNAPPY),
        "lz4" => Ok(Compression::LZ4_RAW),
        "uncompressed" => Ok(Compression::UNCOMPRESSED),
        _ => panic!(
            "Valid compression string is not given. Must be one of: BROTLI GZIP ZSTD LZ4 SNAPPY UNCOMPRESSED"
        ),
    }
}

fn collect_column_paths(schema: &SchemaDescriptor) -> Vec<ColumnPath> {
    schema
        .columns()
        .iter()
        .map(|col| col.path().clone())
        .collect()
}
