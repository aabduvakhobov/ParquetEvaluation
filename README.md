# Project Overview
This repository contains implementation of the paper __"In-Depth Evaluation of Parquet for Large Sensor Datasets"__.

## Motivation
Apache Parquet (Parquet) is commonly used for storing high-frequency sensor data. Parquet is highly configurable and has many implementations. However, each implementation uses different default configuration and none of these configurations provide the optimal performance for sensor data regarding ingestion time, storage size and query time. Also we show that it is possible to configure Parquet depending on the target workload.

## Overview
This project performs an in-depth evaluation of Apache Parquet with large real-life sensor datasets. We evaluate the following:
1. We evaluate the impact of default configurations of standalone Parquet implementations on ingestion time, storage size and query time.
2. We evaluate the impact each Parquet parameter individually on ingestion time, storage size and query time.
3. We evaluate the impact of each Parquet parameter in combination with the other parameters on ingestion time, storage size and query time.
4. We identify relevant dataset properties as a way to configure Parquet for the given optimization target.

## Findings
We find that currently used default configurations of Parquet implementations do not provide optimal performance on any metric (ingestion time, storage size and query time). We find that with better Parquet tuning, ingestion time, storage size and query time can be decreased by up to 1.25x, 2.5x and 1.14x compared to the used default configurations, respectively. In case of optimizing for each metric individually, ingestion time, storage size and query time can be decreased by up to 2.5x, 3.6x and 1.2x, respectively. We show that it is possible to choose the rigth configuration depending on dataset properties and achieve a balance between all the metrics.

# Repository Structure
| Component | Contents |
|---|---|
| [data_preparation/](data_preparation/) | Scripts that turn the raw datasets into the Parquet files used in the experiments. |
| [experiments/](experiments/) | Parquet writers in Rust, C++, Go and Java, plus the Python experiment runner. |
| [visualization/](visualization/) | Figure generation for the paper. |

# Environment Setup
The experiments were run on Linux (x86-64, Ubuntu 24.04). The writers in Rust, C++, Go and Java are built from source, so you need a toolchain for each of them. The scripts also need `git`, which applies the patches in `experiments/writers/rust/patches/`, and `python3`.

## Prerequisites

| Writer | Required | Tested with |
|---|---|---|
| Rust | Rust 1.85 or newer (edition 2024) with Cargo; a C compiler, because SQLite is compiled into the binary (`rusqlite` with the `bundled` feature) | rustc / cargo 1.96.0 |
| C++ | CMake 3.18 or newer; a C and C++17 compiler; `make` or Ninja; static glibc (`libc6-dev` / `glibc-static`); internet access on the first build | g++ 13.3.0, CMake 3.28.3, glibc 2.39 |
| Go | Go 1.26.5 or newer (from `go.mod`) | go 1.26.5 |
| Java | **Java 21** (JDK) and Maven 3.9 or newer | OpenJDK 21.0.12, Maven 3.9.16 |
| Runner, figures | Python 3.10 or newer, with the packages in [requirements.txt](requirements.txt) | Python 3.12.3 |

You don't need to install Arrow C++ or SQLite. On the first configure, the C++ build downloads Arrow C++ (commit `68c8cec`, `apache-arrow-26.0.0.dev-10`) and the SQLite amalgamation, checks both against SHA-256 hashes, and builds them with their bundled dependencies. This takes a while; on a 16-core machine allow several minutes. The result is a fully static binary; see [experiments/writers/cpp/README.md](experiments/writers/cpp/README.md). The Go and Java dependencies, including SQLite, are downloaded by `go build` and `mvn package`.

### Installation on Ubuntu/Debian

```bash
# C/C++ compiler, make, static glibc, CMake, git
sudo apt install build-essential cmake git

# Rust (rustup installs the latest stable toolchain)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Java 21 and Maven (use Maven from https://maven.apache.org if your distribution's is older than 3.9)
sudo apt install openjdk-21-jdk maven

# Go: the distribution package is usually older than 1.26.5, so install it from https://go.dev/dl/
curl -LO https://go.dev/dl/go1.26.5.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.26.5.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

Check the versions:

```bash
rustc --version && cargo --version
g++ --version && cmake --version
go version
java -version && mvn -v
```

Once the toolchains are installed, [experiments/scripts/run_native_writers.sh](experiments/scripts/run_native_writers.sh) builds all four writers. To build them one by one, see [experiments/README.md](experiments/README.md#1-build-the-writers).

## Python packages
Install the packages in [requirements.txt](requirements.txt):

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

# Datasets
The following table shows the summary of datasets used in the experiments.

| Dataset | Rows | SI | Value Columns | Size (Parquet) | Period | Data Type | Null (%) |
|:-------:|:----:|:--:|:-------------:|:--------------:|:------:|:---------:|:--------:|
| BLUED | ~6.9B | ~83 μs | 5 | 62 GB | 1 week | 32-bit float | 0 |
| GREEND | ~110M | 1 s | 30 | 1.2 GB | 1 year | 32-bit float | ~71 |
| REDD | ~83M | ~110 ms | 1 | 651 MB | ~42 days | 32-bit float | 0 |
| NEON Wind | ~198M | ~2 m | 3 | 2 GB | ~7 years | 32-bit float | 0 |

You can find pre-processed versions of all the datasets from the [Harvard Dataverse repository](https://doi.org/10.7910/DVN/SZAHLT).

Citation: `Abduvakhobov, Abduvoris; Jensen, Søren Kejser; Thomsen, Christian; Torben Bach Pedersen, 2026, "Replication Data for: In-depth Evaluation of Parquet for Large Sensor Datasets", https://doi.org/10.7910/DVN/SZAHLT, Harvard Dataverse, V2`

The experiment scripts read each dataset from the `data/` directory at the root of this repository, in a subdirectory named after the dataset in lower case:

```
data/
├── blued/        *.parquet
├── greend/       *.parquet
├── redd/         *.parquet
└── neon_wind/    *.parquet
```

For example, the REDD files go in `data/redd/`. If you keep the datasets elsewhere, a symbolic link works too: `ln -s /path/to/redd data/redd`. The `data/` directory is ignored by git.


# Running Experiments
See [experiments/README.md](experiments/README.md) for building the writers and running:
1. the default configurations of the C++, Go, Java and Rust implementations,
2. each Parquet parameter in isolation,
3. combinations of parameters.

# Evaluation in the Paper
See [visualization/README.md](visualization/README.md). First run the experiments, which write their SQLite databases to `data/output/`. Then create the figure datasets from them with [visualization/create_figure_datasets.py](visualization/create_figure_datasets.py), and draw the figures with the plotting functions in [visualization/paper_figures.ipynb](visualization/paper_figures.ipynb).
