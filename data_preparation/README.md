
# Data Preparation

These scripts show how the raw datasets were turned into the Parquet files used in the experiments. Pre-processed versions of the datasets are available for download (see [Datasets](../README.md#datasets) in the top-level README), so you don't need to run the preprocessing yourself.

## Preparing the downloaded datasets

With the downloaded pre-processed datasets, the only step needed is to merge the files of BLUED and 2D-Wind (NEON Wind) into one Parquet file each, written to `<repo>/data/<dataset>/`. From the root of the repository:

```bash
python data_preparation/common/merge_parquet_files.py /path/to/downloaded/blued blued data/blued
python data_preparation/common/merge_parquet_files.py /path/to/downloaded/neon_wind neon_wind data/neon_wind
```

[common/merge_parquet_files.py](common/merge_parquet_files.py) writes `<save_path>/<dataset_name>.parquet` with pyarrow's default configuration. It streams one row group at a time, so it also works for BLUED. The input files must have the same schema.

## Preprocessing of the raw datasets (for reference)

The scripts below converted each dataset from its original format into Parquet. They are included for information only, to document how the published Parquet files were produced. They are kept as they were used and aren't maintained as part of the experiment pipeline.

| Dataset | Original format | Scripts, in the order they were applied |
|---|---|---|
| BLUED | bzip2-compressed text files (`*.txt.bz2`), each with a header and a block of samples | 1. [blued/preprocess_blued_dataset.py](blued/preprocess_blued_dataset.py): converts each file to Parquet. The timestamp (`time`, microseconds) is the start date and time from the file header plus each sample's `X_Value` offset. The values `current_a`, `current_b` and `voltage` are stored as 32-bit floats.<br>2. [common/sort_using_polars.py](common/sort_using_polars.py): sorts by timestamp.<br>3. [blued/partition_blued.py](blued/partition_blued.py): splits the result into parts of about 2 GiB along row-group boundaries. |
| GREEND | CSV files per household and day | [greend/](greend/): raw CSVs → long format → wide multivariate time series. The steps are in [greend/README.md](greend/README.md). |
| REDD | High-frequency raw data archive (`*.tar.bz2`) of `.dat` files, each row a timestamp followed by a waveform of 275 current values | 1. [redd/preprocess_high_freq_redd_dataset.py](redd/preprocess_high_freq_redd_dataset.py): extracts the archive and flattens each waveform into one row per value (`waveform_values_amps`, 32-bit float). The 275 values of a waveform get timestamps spaced evenly up to the next waveform's timestamp (`time`, milliseconds). Writes one Parquet file per `.dat` file.<br>2. [common/sort_using_polars.py](common/sort_using_polars.py): sorts by timestamp. |
| 2D-Wind (NEON Wind) | NEON 2D wind speed and direction CSV files, per site and month | [2d_wind/preprocess_neon_datasets.py](2d_wind/preprocess_neon_datasets.py) with `wind_speed`: reads the 2-minute CSV files of each site with DuckDB and keeps `endDateTime`, `windSpeedMean`, `windSpeedMinimum` and `windSpeedMaximum`. It drops rows with missing values, stores the values as 32-bit floats and writes one Parquet file per site. |

Usage of each script, as printed by the script itself:

```bash
python blued/preprocess_blued_dataset.py /path/to/blued_files /path/to/save/files
python redd/preprocess_high_freq_redd_dataset.py /path/to/file.tar.bz2 /path/to/save/files
python 2d_wind/preprocess_neon_datasets.py /path/to/neon_root /path/to/save/files wind_speed
python common/sort_using_polars.py timestamp_column /path/to/parquet_files save_file.parquet
python blued/partition_blued.py /path/to/files /path/to/output_dir [target_size_gib=2]
```

The preprocessing scripts use packages from [requirements.txt](../requirements.txt): pandas, pyarrow, polars and DuckDB.
