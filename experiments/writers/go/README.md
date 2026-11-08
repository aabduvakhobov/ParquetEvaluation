# Go Parquet writer

Writes a Parquet file with the default writer properties of Arrow Go (arrow-go v18.6.0).

## Compile
```bash
CGO_ENABLED=0 go build
```

`CGO_ENABLED=0` produces a fully static, self-contained binary (`ldd` prints "not a dynamic executable"). A plain `go build` also works, but the binary then links glibc dynamically. Either way the writer behaves the same: it uses Go's own memory allocator, and the SQLite driver (`modernc.org/sqlite`) is pure Go.

Requires Go 1.26.5 or newer.

## Run
```bash
./parquet-writer-go "/path/to/parquet_files_dir/*" /path/to/new_file.parquet <dataset-name> /path/to/default_writers.db
```

The run is recorded in the `writer_runs` table of the SQLite database. See [experiments/README.md](../../README.md#default-configurations-of-the-parquet-implementations).
