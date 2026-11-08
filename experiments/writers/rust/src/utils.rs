use crate::dataset_configurable::Dataset;

use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};

use glob::glob;
use std::collections::HashMap;
use std::fs::File;
use std::io::Error;
use std::{path::Path, path::PathBuf, process};

pub fn read_user_input(
    args: Vec<String>,
) -> (
    String,
    String,
    String,
    String,
    String,
    String,
    HashMap<String, String>,
) {
    if args.len() < 8 {
        eprintln!(
            "Usage: {} table_name error_bound[add % for relative] /path/to/parquet_files /path/to/save unique_file_name /path/to/sqlite_db KEY=VALUE[ COMPRESSION=BROTLI COMPRESSION_LEVEL=1 STATISTICS_ENABLED=true or BLOOM_FILTER_ENABLED=true ]",
            &args[0]
        );
        std::process::exit(1);
    }
    let dataset_name = &args[1];
    let str_error_bound = &args[2].to_string();
    let parquet_files_path = &args[3].to_string();
    let path_to_save = &args[4].to_string();
    let unique_file_name = &args[5].to_string();
    let path_to_sqlite_db = &args[6].to_string();

    // key=value pairs from index 5
    let mut config_map = std::collections::HashMap::new();

    for kv in &args[7..] {
        if let Some((key, value)) = kv.split_once('=') {
            config_map.insert(key.to_string(), value.to_string());
        } else {
            eprintln!("Invalid argument '{}': expected KEY=VALUE format", kv);
            std::process::exit(1);
        }
    }

    (
        dataset_name.to_string(),
        str_error_bound.to_string(),
        parquet_files_path.to_string(),
        path_to_save.to_string(),
        unique_file_name.to_string(),
        path_to_sqlite_db.to_string(),
        config_map,
    )
}

pub fn get_parquet_files(str_path: &String) -> Vec<PathBuf> {
    let mut parquet_files: Vec<PathBuf> = Vec::new();
    let path = Path::new(str_path);
    if path.is_dir() {
        let full_path = format!("{}/{}", str_path, "*.parquet");
        println!("{}", full_path);
        for entry in glob(full_path.as_str()).expect("Failed to read glob pattern") {
            match entry {
                Ok(path) => parquet_files.push(path),
                Err(e) => println!("Not a parquet file: {:?}", e),
            }
        }
    } else if path.is_file() && str_path.ends_with(".parquet") {
        parquet_files.push(path.to_path_buf());
    } else {
        eprintln!("Directory: {} does not have parquet files", str_path);
        process::exit(1)
    }
    parquet_files
}

pub fn create_last_value_map(num_columns: usize) -> HashMap<usize, f32> {
    let mut last_values = HashMap::new();
    for i in 0..num_columns {
        last_values.insert(i, f32::INFINITY);
    }
    last_values
}
// returns file size in bytes, using the filesystem metadata.
// If the file cannot be accessed an error message will be printed and 0 is returned.
pub fn check_parquet_file_size(full_file_path: &Path) -> u64 {
    match std::fs::metadata(&full_file_path) {
        Ok(meta) => meta.len(),
        Err(e) => {
            eprintln!(
                "failed to read metadata for {}: {}",
                full_file_path.display(),
                e
            );
            0
        }
    }
}

// Returns a tuple that contains two vectors: arrow writers names and arrow writers
pub fn create_parquet_writers(
    dataset: &Dataset,
) -> Result<Vec<(String, ArrowWriter<File>)>, Error> {
    match dataset.get_default_compression() {
        Compression::BROTLI(brotli_level) => {
            let brotli_plain = File::create(format!(
                "{}/{}-{}-BROTLI({})-PLAIN.parquet",
                dataset.file_save_path,
                dataset.name,
                dataset.error_bound,
                brotli_level.compression_level()
            ))?;
            let brotli_byte_stream = File::create(format!(
                "{}/{}-{}-BROTLI({})-BYTE_STREAM_SPLIT.parquet",
                dataset.file_save_path,
                dataset.name,
                dataset.error_bound,
                brotli_level.compression_level()
            ))?;
            let brotli_rle = File::create(format!(
                "{}/{}-{}-BROTLI({})-RLE_DICTIONARY.parquet",
                dataset.file_save_path,
                dataset.name,
                dataset.error_bound,
                brotli_level.compression_level()
            ))?;
            Ok(vec![
                (
                    format!(
                        "{}-{}-BROTLI({})-PLAIN.parquet",
                        dataset.name,
                        dataset.error_bound,
                        brotli_level.compression_level()
                    ),
                    ArrowWriter::try_new(
                        brotli_plain,
                        dataset.arrow_schema.clone(),
                        Some(
                            dataset.create_parquet_writer_properties_batch(Encoding::PLAIN, false),
                        ),
                    )
                    .unwrap(),
                ),
                (
                    format!(
                        "{}-{}-BROTLI({})-BYTE_STREAM_SPLIT.parquet",
                        dataset.name,
                        dataset.error_bound,
                        brotli_level.compression_level()
                    ),
                    ArrowWriter::try_new(
                        brotli_byte_stream,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(
                            Encoding::BYTE_STREAM_SPLIT,
                            false,
                        )),
                    )
                    .unwrap(),
                ),
                (
                    format!(
                        "{}-{}-BROTLI({})-RLE_DICTIONARY.parquet",
                        dataset.name,
                        dataset.error_bound,
                        brotli_level.compression_level()
                    ),
                    ArrowWriter::try_new(
                        brotli_rle,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(Encoding::PLAIN, true)),
                    )
                    .unwrap(),
                ),
            ])
        }
        Compression::ZSTD(zstd_level) => {
            let zstd_plain = File::create(format!(
                "{}/{}-{}-ZSTD({})-PLAIN.parquet",
                dataset.file_save_path,
                dataset.name,
                dataset.error_bound,
                zstd_level.compression_level()
            ))?;
            let zstd_byte_stream = File::create(format!(
                "{}/{}-{}-ZSTD({})-BYTE_STREAM_SPLIT.parquet",
                dataset.file_save_path,
                dataset.name,
                dataset.error_bound,
                zstd_level.compression_level()
            ))?;
            let zstd_rle = File::create(format!(
                "{}/{}-{}-ZSTD({})-RLE_DICTIONARY.parquet",
                dataset.file_save_path,
                dataset.name,
                dataset.error_bound,
                zstd_level.compression_level()
            ))?;
            Ok(vec![
                (
                    format!(
                        "{}-{}-ZSTD({})-PLAIN.parquet",
                        dataset.name,
                        dataset.error_bound,
                        zstd_level.compression_level()
                    ),
                    ArrowWriter::try_new(
                        zstd_plain,
                        dataset.arrow_schema.clone(),
                        Some(
                            dataset.create_parquet_writer_properties_batch(Encoding::PLAIN, false),
                        ),
                    )
                    .unwrap(),
                ),
                (
                    format!(
                        "{}-{}-ZSTD({})-BYTE_STREAM_SPLIT.parquet",
                        dataset.name,
                        dataset.error_bound,
                        zstd_level.compression_level()
                    ),
                    ArrowWriter::try_new(
                        zstd_byte_stream,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(
                            Encoding::BYTE_STREAM_SPLIT,
                            false,
                        )),
                    )
                    .unwrap(),
                ),
                (
                    format!(
                        "{}-{}-ZSTD({})-RLE_DICTIONARY.parquet",
                        dataset.name,
                        dataset.error_bound,
                        zstd_level.compression_level()
                    ),
                    ArrowWriter::try_new(
                        zstd_rle,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(Encoding::PLAIN, true)),
                    )
                    .unwrap(),
                ),
            ])
        }
        Compression::LZ4_RAW => {
            let lz4_plain = File::create(format!(
                "{}/{}-{}-LZ4-PLAIN.parquet",
                dataset.file_save_path, dataset.name, dataset.error_bound
            ))?;
            let lz4_byte_stream = File::create(format!(
                "{}/{}-{}-LZ4-BYTE_STREAM_SPLIT.parquet",
                dataset.file_save_path, dataset.name, dataset.error_bound
            ))?;
            let lz4_rle = File::create(format!(
                "{}/{}-{}-LZ4-RLE_DICTIONARY.parquet",
                dataset.file_save_path, dataset.name, dataset.error_bound
            ))?;
            Ok(vec![
                (
                    format!("{}-{}-LZ4-PLAIN.parquet", dataset.name, dataset.error_bound),
                    ArrowWriter::try_new(
                        lz4_plain,
                        dataset.arrow_schema.clone(),
                        Some(
                            dataset.create_parquet_writer_properties_batch(Encoding::PLAIN, false),
                        ),
                    )
                    .unwrap(),
                ),
                (
                    format!(
                        "{}-{}-LZ4-BYTE_STREAM_SPLIT.parquet",
                        dataset.name, dataset.error_bound
                    ),
                    ArrowWriter::try_new(
                        lz4_byte_stream,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(
                            Encoding::BYTE_STREAM_SPLIT,
                            false,
                        )),
                    )
                    .unwrap(),
                ),
                (
                    format!(
                        "{}-{}-LZ4-RLE_DICTIONARY.parquet",
                        dataset.name, dataset.error_bound
                    ),
                    ArrowWriter::try_new(
                        lz4_rle,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(Encoding::PLAIN, true)),
                    )
                    .unwrap(),
                ),
            ])
        }
        _ => {
            let snappy_byte_stream_split = File::create(format!(
                "{}/{}-{}-SNAPPY-BYTE_STREAM_SPLIT.parquet",
                dataset.file_save_path, dataset.name, dataset.error_bound
            ))?;

            let snappy_rle = File::create(format!(
                "{}/{}-{}-SNAPPY-RLE_DICTIONARY.parquet",
                dataset.file_save_path, dataset.name, dataset.error_bound
            ))?;
            Ok(vec![
                (
                    format!(
                        "{}-{}-SNAPPY-BYTE_STREAM_SPLIT.parquet",
                        dataset.name, dataset.error_bound
                    ),
                    ArrowWriter::try_new(
                        snappy_byte_stream_split,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(
                            Encoding::BYTE_STREAM_SPLIT,
                            false,
                        )),
                    )
                    .unwrap(),
                ),
                (
                    format!(
                        "{}-{}-SNAPPY-RLE_DICTIONARY.parquet",
                        dataset.name, dataset.error_bound
                    ),
                    ArrowWriter::try_new(
                        snappy_rle,
                        dataset.arrow_schema.clone(),
                        Some(dataset.create_parquet_writer_properties_batch(Encoding::PLAIN, true)),
                    )
                    .unwrap(),
                ),
            ])
        }
    }
}
