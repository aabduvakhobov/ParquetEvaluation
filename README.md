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

# Environment Setup

``

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

# Evaluation in the Paper
