# Project Overview
This repository contains the artifacts for reproducing the results in the paper __"In-Depth Evaluation of Parquet for Large Sensor Datasets"__.

## Motivation
Apache Parquet (Parquet) is commonly used for storing high-frequency sensor data. Parquet is highly configurable and has many implementations. However, each implementation uses different default configuration and none of these configurations provide the optimal performance for sensor data regarding ingestion time, storage size and query time. Also we show that it is possible to configure Parquet depending on the dataset properties in a way that all the metrics (ingestion time, storage size and query time) are balanced.

## Overview
This project performs an in-depth evaluation of Apache Parquet with large real-life sensor datasets. We evaluate the following:
1. We evaluate the impact of default configurations of standalone Parquet implementations on ingestion time, storage size and query time.
2. We evaluate the impact each Parquet parameter individually on ingestion time, storage size and query time.
3. We evaluate the impact of each Parquet parameter in combination with the other parameters on ingestion time, storage size and query time.
4. We identify relevant dataset properties as a way to configure Parquet for the given optimization target.

## Findings
We find that currently used default configurations of Parquet implementations do not provide optimal performance on any metric (ingestion time, storage size and query time). We find that with better Parquet tuning, ingestion time, storage size and query time can be decreased by up to 1.25x, 3.1x and 1.07x compared to the used default configurations, respectively. In case of optimizing for each metric individually, ingestion time, storage size and query time can be decreased by up to 2.5x, 3.6x and 1.2x, respectively. We show that it is possible to choose the rigth configuration depending on dataset properties and achieve a balance between all the metrics.

# Repository Structure
| Component | Contents |
|---|---|
| [data_preparation/](data_preparation/) | Scripts that show how the raw datasets were preprocessed into the Parquet files that are used in the experiments. You are not expected to do that. Go to Datasets section to find the link to download preprocessed datasets. |
| [experiments/](experiments/) | Parquet writers in Rust, C++, Go and Java, plus the Python experiment runner. |
| [visualization/](visualization/) | For generating datasets used for figures in the paper. |
| [dataset_properties/](dataset_properties/) | Analysis of the dataset properties that explain which Parquet value encoding gives the smallest files. |
| [paper_table/](paper_table/) | Additional tables referred to in the paper, in Markdown ([markdown/](paper_table/markdown/)) and LaTeX ([latex/](paper_table/latex/)). |

# Environment Setup
The experiments were run on Linux (x86-64, Ubuntu 24.04). The writers in Rust, C++, Go and Java are built from source, so you need a toolchain for each of them. The scripts also need `git`, which applies the patches in `experiments/writers/rust/patches/`, and `python3`.

## Prerequisites

| Writer | Required | Tested with |
|---|---|---|
| Rust | Rust 1.85 or newer (edition 2024) with Cargo; a C compiler, because SQLite is compiled into the binary (`rusqlite` with the `bundled` feature) | rustc / cargo 1.96.0 |
| C++ | CMake 3.25 or newer; a C compiler and a C++20 compiler (gcc 12 or newer), both required by Arrow C++; `make` or Ninja; static glibc (`libc6-dev` / `glibc-static`); internet access on the first build | g++ 13.3.0, CMake 3.28.3, glibc 2.39 |
| Go | Go 1.26.5 or newer (from `go.mod`) | go 1.26.5 |
| Java | **Java 21** (JDK) and Maven 3.9 or newer | OpenJDK 21.0.12, Maven 3.9.16 |
| Runner, figures | Python 3.10 or newer, with the packages in [requirements.txt](requirements.txt) | Python 3.12.3 |

You don't need to install Arrow C++ or SQLite. On the first configure, the C++ build downloads Arrow C++ (commit `68c8cec`, `apache-arrow-26.0.0.dev-10`) and the SQLite amalgamation, checks both against SHA-256 hashes, and builds them with their bundled dependencies. This takes a while; on a 16-core machine allow several minutes. The result is a fully static binary; see [experiments/writers/cpp/README.md](experiments/writers/cpp/README.md). The Go and Java dependencies, including SQLite, are downloaded by `go build` and `mvn package`.

### Installation on Ubuntu/Debian

The commands below are for Ubuntu 24.04. On older releases, the packaged CMake and gcc are too old for Arrow C++: Ubuntu 20.04, for example, has CMake 3.16 and gcc 9, and Ubuntu 22.04 has CMake 3.22 and gcc 11. Install newer versions as shown after the block below.

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
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc

# source your PATH for changes to take effect
source ~/.bashrc
```

If you need newer CMake and gcc:

```bash
# CMake 3.25 or newer, from PyPI (installs to ~/.local/bin, which must come before /usr/bin in PATH)
python3 -m pip install --user --upgrade cmake
echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc

# gcc/g++ 12 or newer: use your distribution's gcc-12 package if it has one (Ubuntu 22.04),
# or the ubuntu-toolchain-r/test PPA (Ubuntu 20.04), then point the build at it
sudo add-apt-repository ppa:ubuntu-toolchain-r/test && sudo apt update
sudo apt install gcc-13 g++-13
echo 'export CC=gcc-13 CXX=g++-13' >> ~/.bashrc

# source your PATH for changes to take effect
source ~/.bashrc
```

Set `CC` and `CXX` before the first configure of the C++ writer. CMake caches the compiler, so if you configured before, delete `experiments/writers/cpp/build/`.

Check the versions:

```bash
rustc --version && cargo --version
g++ --version && cmake --version
go version
java -version && mvn -v
```

Once the toolchains are installed, [experiments/scripts/run_native_writers.sh](experiments/scripts/run_native_writers.sh) builds all four writers. To build them one by one, see [experiments/README.md](experiments/README.md#1-build-the-writers).

## Python packages
Install Miniconda, create a conda environment with Python 3.12 and install the packages in [requirements.txt](requirements.txt) into it. Run the commands from the root of this repository:

```bash
curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash ~/Miniconda3-latest-Linux-x86_64.sh

# Load conda into the current shell (the installer adds it to ~/.bashrc)
source ~/.bashrc

# Create and activate the environment
conda create -n parquet-evaluation python=3.12
conda activate parquet-evaluation

# Install the project dependencies
pip install -r requirements.txt
```

Activate the environment with `conda activate parquet-evaluation` in every new shell before you run the experiment or visualization scripts.

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
Please note that we neon_wind for 2D-Wind dataset to ensure consistency and avoid errors across all implementations.
For example, the REDD files go in `data/redd/`. If you keep the datasets elsewhere, a symbolic link works too: `ln -s /path/to/redd data/redd`. The `data/` directory is ignored by git.

The downloaded BLUED and 2D-Wind (NEON Wind) datasets consist of many Parquet files. After downloading them, merge each into a single Parquet file with [data_preparation/common/merge_parquet_files.py](data_preparation/common/merge_parquet_files.py), and write the merged file to the dataset's directory under `data/`. Download them to a directory outside `data/`, so that only the merged file ends up in `data/<dataset>/`:

```bash
python data_preparation/common/merge_parquet_files.py /path/to/downloaded/blued blued data/blued
python data_preparation/common/merge_parquet_files.py /path/to/downloaded/neon_wind neon_wind data/neon_wind
```

The script writes `data/<dataset>/<dataset>.parquet` with pyarrow's default configuration. It streams the files one row group at a time, so it does not run out of memory on BLUED (62 GB).


# Running Experiments
See [experiments/README.md](experiments/README.md) for building the writers and running:
1. the default configurations of the C++, Go, Java and Rust implementations,
2. each Parquet parameter in isolation,
3. combinations of parameters.

# Evaluation in the Paper
See [visualization/README.md](visualization/README.md). First run the experiments, which write their SQLite databases to `data/output/`. Then create the figure datasets from them with [visualization/create_figure_datasets.py](visualization/create_figure_datasets.py), and draw the figures with the plotting functions in [visualization/paper_figures.ipynb](visualization/paper_figures.ipynb).

# Dataset Properties
[dataset_properties/](dataset_properties/) explains, from properties of the data, why a different Parquet value encoding gives the smallest files for each dataset: BYTE_STREAM_SPLIT for REDD, and Dict-RLE for BLUED, GREEND and 2D-Wind. It also explains which dictionary page limit is effective. [data_properties.py](dataset_properties/data_properties.py) measures properties such as distinct values, repeats and byte entropies per row group. It also rewrites every float column chunk with each value encoding and block compressor to measure the real Parquet sizes.

We already share precomputed dataset properties in the form of CSV files in the [dataset_properties/](dataset_properties/) . If you want to do that again refer to the following.
The script reads the original datasets from `data/<dataset>/` (see [Datasets](#datasets)). Its results are saved as CSV files in the folder, so you only need the datasets to recompute them:

```bash
cd dataset_properties
python -I data_properties.py
```

See [dataset_properties/README.md](dataset_properties/README.md) for the method, the measured properties and the findings, and [dataset_properties/dataset_property_table.md](dataset_properties/dataset_property_table.md) for the summary table.

# Our Setup
We run all experiments on a server with an AMD EPYC 7302P 16 Cores and 32 Threads, 256 GiB DDR4 3200 MHz RAM, 8 SATA SSDs, Ubuntu 20.04.6 LTS, and the Rust crate parquet v58.0.0. To analyze query time in detail, we use both DuckDB v1.2.1 and DataFusion v53.0.0 .