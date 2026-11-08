// TODO: Write all statistics to the same sqlite database (i.e., file) but to different tables
// Standardize the attribute names accros sqlite tables
// TODO: Don't iterate over each page in a parquet and write its data to sqlite, better summarize the statistics over iteration
// and only write the summary to sqlite

// For checking if the code is inlined and vectorized, first compile the binary with debug flags and dump the assembly code

mod dataset_configurable;
mod macaquev;
mod utils;

use parquet::file::properties::WriterProperties;
use std::fs::File;
use utils::get_parquet_files;

use std::time::{Instant, Duration};
use std::{env, process};

use parquet::arrow::{ArrowWriter, arrow_reader};

const WRITER_NAME: &str = "rust";

/// Appends one row to the writer_runs table shared by the C++, Go, Java and Rust default writers,
/// creating the table if it does not exist yet.
fn record_run(
    sqlite_path: &str,
    dataset_name: &str,
    total_write_time: f64,
    file_size_in_bytes: u64,
) -> rusqlite::Result<()> {
    let conn = rusqlite::Connection::open(sqlite_path)?;
    conn.busy_timeout(Duration::from_secs(60))?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS writer_runs (
            dataset_name TEXT, writer TEXT, total_write_time REAL, file_size_in_bytes INTEGER)",
        [],
    )?;
    conn.execute(
        "INSERT INTO writer_runs (dataset_name, writer, total_write_time, file_size_in_bytes)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![dataset_name, WRITER_NAME, total_write_time, file_size_in_bytes as i64],
    )?;
    Ok(())
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    // read user parameters
    let args: Vec<String> = env::args().collect();

    if args.len() != 5 {
        eprintln!(
            "Usage: {} /path/to/parquet_files /path/to/save/file.parquet dataset_name /path/to/sqlite_db",
            &args[0]
        );
        process::exit(1)
    }
    let dataset_name = &args[3];
    let sqlite_path = &args[4];
    let parquet_files = get_parquet_files(&args[1]);
    // Set schema
    let file = File::open(parquet_files[0].clone())?;
    // let mut last_values_map = create_last_value_map(builder.parquet_schema().num_columns());
    let builder = arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    // Create row group buffer
    let schema = builder.schema();
    // parse configurations
    // Create the Dataset instance using the user input struct
    let file = File::create(args[2].clone())?;
    let mut parquet_writer = ArrowWriter::try_new(file, schema.clone(), Some(WriterProperties::default()))?;
    println!("All files: {}", parquet_files.len());
    let mut total_time = Duration::ZERO;

    let start = Instant::now();
    for parquet_file_path in parquet_files {
        // read parquet files in the given path
        let file = File::open(parquet_file_path.clone())?;
        // let mut last_values_map = create_last_value_map(builder.parquet_schema().num_columns());
        let builder = arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        builder.schema();
        // iterate over recordbatches. Default record batch size is 1024 and it won't be changed
        // to minimize the impact of its size on data page sizes, which we experienced in our experiments.
        let mut reader = builder.build()?;

        while let Some(batch) = reader.next() {
            let batch = batch?;
            parquet_writer.write(&batch)?;
        }
    }
    // read the parquet files stored on disk and read what was actually stored
    parquet_writer.close()?;
    total_time += start.elapsed();
    println!("Ingestion time: {} seconds ", total_time.as_secs());

    // Recorded after the timer stopped, so it does not affect the measured time.
    let file_size_in_bytes = std::fs::metadata(&args[2])?.len();
    if let Err(e) = record_run(sqlite_path, dataset_name, total_time.as_secs_f64(), file_size_in_bytes) {
        eprintln!("Failed to record run in {}: {}", sqlite_path, e);
        process::exit(1)
    }
    println!("Recorded run in {} (table writer_runs)", sqlite_path);
    Ok(())
}
