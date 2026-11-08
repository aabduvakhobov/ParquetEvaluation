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
Toolchains needed to build the Parquet writers, with the versions they were tested with:

| Writer | Requirements | Tested with |
|---|---|---|
| Rust | Rust with edition 2024 support (1.85 or newer) | rustc 1.96.0 |
| C++ | CMake 3.18+, a C/C++17 compiler, internet on first build. Arrow C++ is downloaded and built automatically, and the binary is fully static; see [experiments/writers/cpp](experiments/writers/cpp/README.md). | g++ 13.3, CMake 3.28, Arrow 26.0.0.dev (`68c8cec`) |
| Go | Go 1.26.5 or newer (from `go.mod`) | go 1.26.5 |
| Java | **Java 21** and Maven 3.9+ | OpenJDK 21.0.12, Maven 3.9.16 |

Python 3 with the packages in [requirements.txt](requirements.txt):

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

We uploaded pre-processed versions of all the datasets in this repository [link](). 


# Running Experiments
See [experiments/README.md](experiments/README.md) for building the writers, configuring dataset paths and running:
1. the default configurations of the C++, Go, Java and Rust implementations,
2. each Parquet parameter in isolation,
3. combinations of parameters.

# Evaluation in the Paper
See [visualization/README.md](visualization/README.md). The figures can be regenerated from the aggregated results in `visualization/data/`, or from the raw result databases written by the experiment runner.
