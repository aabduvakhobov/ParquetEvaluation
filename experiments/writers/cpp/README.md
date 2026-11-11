# C++ Parquet writer

Writes a Parquet file with the default writer properties of Arrow C++.

## Compile
```bash
cmake -DCMAKE_BUILD_TYPE=Release -S . -B build
cmake --build build
```

You don't need to install Arrow. On the first configure, [CMakeLists.txt](CMakeLists.txt) downloads Arrow C++ and builds it under `build/arrow/`. It uses the commit the paper's results were produced with, `68c8cec` (`apache-arrow-26.0.0.dev-10`), and checks the download against a SHA-256 hash.

The first configure takes a while because Arrow and its bundled dependencies are compiled. It needs internet access to download Arrow and those dependencies. Later configures reuse the Arrow build, and the logs are in `build/arrow/arrow-*.log`.

Arrow is built with the same features as the paper's build: mimalloc, SSE4.2 with runtime SIMD dispatch, IPC and Parquet. All compression codecs (ZSTD, ZLIB, Snappy, LZ4, Brotli) are enabled, so input files compressed with any of them can be read. This doesn't change the writer's default properties.

| CMake option | Default | Effect |
|---|---|---|
| `PARQUET_WRITER_BUILD_ARROW` | `ON` | Downloads and builds Arrow. Set it to `OFF` and pass `-DCMAKE_PREFIX_PATH=/path/to/arrow` to use an installed Arrow instead. |
| `ARROW_BUILD_ROOT` | `build/arrow` | Where Arrow is downloaded, built and installed. Point several build folders at one directory to build Arrow only once. |

## Self-contained binary
By default the writer is linked fully statically: Arrow, Parquet, their bundled compression libraries, the C++ runtime and glibc. The resulting `build/parquet_writer_cpp` has no runtime dependencies (`ldd` prints "not a dynamic executable"). You can copy it to any x86-64 Linux machine and run it there.

| CMake option | Default | Effect |
|---|---|---|
| `PARQUET_WRITER_STATIC` | `ON` | Links Arrow, Parquet and the C++ runtime statically. `OFF` uses shared libraries, which requires an installed Arrow with shared libraries (`PARQUET_WRITER_BUILD_ARROW=OFF`). |
| `PARQUET_WRITER_FULLY_STATIC` | `ON` | Also links glibc statically. With `OFF`, the target machine needs glibc at least as new as the build machine's. With glibc older than 2.34 (for example Ubuntu 20.04), it is turned `OFF` automatically: there, a statically linked glibc doesn't handle threads correctly and the writer crashes with a segmentation fault. |

The fully static build prints a linker warning that `dlopen` needs the shared glibc at runtime. You can ignore it: Arrow only uses `dlopen` for optional plugins such as HDFS, and this writer only reads and writes local files.

## Dependencies
- CMake 3.25 or newer, a C compiler and a C++20 compiler (gcc 12 or newer), as required by Arrow C++. Tested with g++ 13.3 and CMake 3.28.
- Static glibc for the fully static build: `libc6-dev` on Debian/Ubuntu, `glibc-static` on Fedora/RHEL.
- Internet access on the first configure.

## Run
```bash
./build/parquet_writer_cpp "/path/to/parquet_files/" /path/to/output.parquet <dataset-name> /path/to/default_writers.db
```

The run is recorded in the `writer_runs` table of the SQLite database. See [experiments/README.md](../../README.md#default-configurations-of-the-parquet-implementations). SQLite 3.54.1 is downloaded at configure time (checked against a SHA-256 hash) and compiled into the binary, so the writer stays self-contained.
