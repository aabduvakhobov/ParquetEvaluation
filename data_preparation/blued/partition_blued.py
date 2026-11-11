import os
import glob
import sys
import pyarrow.parquet as pq

GIB = 1024 ** 3


def get_files(file_or_files_path: str):
    if os.path.isdir(file_or_files_path):
        files = glob.glob(file_or_files_path + os.sep + "*.parquet")
        files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(file_or_files_path):
        files = [file_or_files_path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return files


def group_row_groups(metadata, target_bytes: int):
    """Packs consecutive row groups into parts of at most target_bytes (compressed).

    A row group larger than target_bytes ends up alone in its own part.
    """
    parts, current, current_bytes = [], [], 0
    for i in range(metadata.num_row_groups):
        rg = metadata.row_group(i)
        rg_bytes = sum(rg.column(j).total_compressed_size for j in range(rg.num_columns))
        if current and current_bytes + rg_bytes > target_bytes:
            parts.append(current)
            current, current_bytes = [], 0
        current.append(i)
        current_bytes += rg_bytes
    if current:
        parts.append(current)
    return parts


def split_parquet_file(file_path: str, output_dir: str, target_bytes: int = 2 * GIB):
    """Splits a Parquet file into parts of about target_bytes without changing the data.

    Splits only at row group boundaries, keeps every row group as it is, and
    writes each column with the codec it has in the source file.
    """
    parquet_file = pq.ParquetFile(file_path)
    metadata = parquet_file.metadata
    first_rg = metadata.row_group(0)
    compression = {first_rg.column(j).path_in_schema: first_rg.column(j).compression
                   for j in range(first_rg.num_columns)}

    name = os.path.splitext(os.path.basename(file_path))[0]
    parts = group_row_groups(metadata, target_bytes)
    for part_idx, row_groups in enumerate(parts, start=1):
        out_path = os.path.join(output_dir, f"{name}_part_{part_idx}.parquet")
        with pq.ParquetWriter(out_path, parquet_file.schema_arrow, compression=compression) as writer:
            for i in row_groups:
                table = parquet_file.read_row_group(i)
                writer.write_table(table, row_group_size=table.num_rows)
        print(f"{out_path}: row groups {row_groups[0]}-{row_groups[-1]}, "
              f"{os.path.getsize(out_path) / GIB:.2f} GiB")
    return len(parts)


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print(f"Usage: {sys.argv[0]} /path/to/files /path/to/output_dir [target_size_gib=2]")
        sys.exit(-1)

    files = get_files(sys.argv[1])
    output_dir = sys.argv[2]
    target_bytes = int(float(sys.argv[3]) * GIB) if len(sys.argv) == 4 else 2 * GIB
    os.makedirs(output_dir, exist_ok=True)

    for file_path in files:
        split_parquet_file(file_path, output_dir, target_bytes)
