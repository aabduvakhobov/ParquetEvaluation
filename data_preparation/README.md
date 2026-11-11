# Data Preparation

These scripts turn the raw datasets into the Parquet files used in the experiments. Pre-processed versions of the datasets are also available for download (see the top-level README).

If you use the downloaded pre-processed datasets, the only step needed is to merge the files of BLUED and 2D-Wind (NEON Wind) into one Parquet file each, written to `<repo>/data/<dataset>/` (see [Datasets](../README.md#datasets) in the top-level README). From the root of the repository:

```bash
python data_preparation/common/merge_parquet_files.py /path/to/downloaded/blued blued data/blued
python data_preparation/common/merge_parquet_files.py /path/to/downloaded/neon_wind neon_wind data/neon_wind
```

```bash
pip install -r ../requirements.txt
```

| Dataset | Scripts |
|---|---|
| GREEND | [greend/](greend/): raw CSVs → long format → wide multivariate time series. The steps are in [greend/README.md](greend/README.md). |
| BLUED | [blued/partition_blued.py](blued/partition_blued.py): splits the BLUED Parquet files into parts of about 2 GiB along row-group boundaries:<br>`python partition_blued.py /path/to/files /path/to/output_dir [target_size_gib=2]` |
| BLUED, 2D-Wind | [common/merge_parquet_files.py](common/merge_parquet_files.py): merges all Parquet files in a directory into `<save_path>/<dataset_name>.parquet` with pyarrow's default configuration. Run it after downloading these datasets. It streams one row group at a time, so it also works for BLUED. The input files must have the same schema:<br>`python merge_parquet_files.py dataset_path dataset_name save_path` |
| BLUED, REDD | [common/sort_using_polars.py](common/sort_using_polars.py): sorts by timestamp, streaming the data to disk:<br>`python sort_using_polars.py timestamp_column /path/to/parquet_files save_file.parquet` |
