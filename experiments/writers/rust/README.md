# Rust Parquet writers

Built on the `parquet` crate. Build with `cargo build --release`.

| Binary | Source | Purpose |
|---|---|---|
| `low-level-writer` | [src/native_writer.rs](src/native_writer.rs) | Low-level writer with fine-grained control over all writer properties, including exact row group sizes. Used by the experiment runner. |
| `default-rust-writer` | [src/default_rust_writer.rs](src/default_rust_writer.rs) | High-level Arrow writer with the crate's default properties. Usage: `default-rust-writer /path/to/parquet_files output.parquet dataset_name /path/to/sqlite_db`. It records its run in the `writer_runs` table, like the C++, Go and Java writers. |

## low-level-writer

```
./target/release/low-level-writer table_name error_bound /path/to/parquet_files /path/to/save unique_file_name /path/to/sqlite_db [KEY=VALUE ...]
```

`error_bound` is the pointwise error bound for bit rewriting (append `%` for a relative bound). All experiments in the paper use `0` (lossless).

| Parameter | Values | Description |
|---|---|---|
| `BLOOM_FILTER_ENABLED` | `true` `false` | Bloom filters for the value columns. |
| `COMPRESSION` | `BROTLI` `GZIP` `LZ4` `ZSTD` `SNAPPY` `UNCOMPRESSED` | Block compressor. |
| `COMPRESSION_LEVEL` | Brotli 0–11, Zstd 1–22, Gzip 1–9 | Compression level, where applicable. Defaults to 1. |
| `SORTING_COLUMNS` | list of columns | Columns to sort by. Defaults to none, since the time series are sorted by timestamp. |
| `DATA_PAGE_SIZE_LIMIT` | bytes | Data page size. |
| `DICTIONARY_PAGE_SIZE_LIMIT` | bytes | Dictionary page size. |
| `DICTIONARY_ENABLED` | `true` `false` | Dictionary encoding for all value columns. Falls back to `PLAIN` when the dictionary grows too large. |
| `MAX_ROW_GROUP_SIZE` | rows | Number of rows per row group. |
| `TIMESTAMP_ENCODING` | `DELTA_BINARY_PACKED` `PLAIN` | Encoding of the i64 timestamps. |
| `VALUE_ENCODING` | `BYTE_STREAM_SPLIT` `PLAIN` | Encoding of the f32 values. |
| `STATISTICS_ENABLED` | `NONE` `CHUNK` `PAGE` | Level of the statistics (zone maps). |
| `OFFSET_INDEX_DISABLED` | `true` `false` | Disables the offset index. |

## Patches

[patches/](patches/) changes the defaults of `low-level-writer` so it reproduces the default configurations of the C++, Go, Java and Rust implementations. [../../scripts/run_with_patches.sh](../../scripts/run_with_patches.sh) applies each patch from the repository root, builds the writer, runs it and reverts the patch. To apply one by hand, run `git apply experiments/writers/rust/patches/<lang>_configs.patch` from the repository root.
