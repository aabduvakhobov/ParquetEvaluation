import polars as pl
import sys

def main(path_to_files, save_file, timestamp_col):

    # 1. Scan the large Parquet file lazily (does not load it into memory)
    lazy_df = pl.scan_parquet(f"{path_to_files}")
    
    # 2. Define the sorting logic
    sorted_lazy_df = lazy_df.sort(timestamp_col)
    
    # 3. Stream the sorted data directly to disk
    sorted_lazy_df.sink_parquet(save_file, compression='zstd')
    
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} timestamp_column_name path_to_parquet_files save_file_name_path")
        sys.exit(-1)
    main(sys.argv[2], sys.argv[3], sys.argv[1])
