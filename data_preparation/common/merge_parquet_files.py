"""Merge all Parquet files in a directory into one Parquet file written with pyarrow's default configuration.

The files are streamed in batches, so memory holds about one output row group at a time (1,048,576 rows, pyarrow's
default row group size), not whole files. This keeps memory bounded even for very large datasets such as BLUED.

Input files are read in sorted file-name order and must have the same schema. The output is written to
<save_path>/<dataset_name>.parquet.

Usage: python merge_parquet_files.py dataset_path dataset_name save_path
"""
import argparse
import glob
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROW_GROUP_SIZE = 1024 * 1024  # pyarrow's default maximum rows per row group
READ_BATCH_SIZE = 64 * 1024


def merge(dataset_path, dataset_name, save_path):
    input_files = sorted(glob.glob(os.path.join(dataset_path, '*.parquet')))
    if not input_files:
        sys.exit(f'No Parquet files found in {dataset_path}')
    os.makedirs(save_path, exist_ok=True)
    output_file = os.path.join(save_path, f'{dataset_name}.parquet')
    if os.path.abspath(output_file) in map(os.path.abspath, input_files):
        sys.exit(f'The output file {output_file} is one of the input files; choose another save_path')

    schema = pq.read_schema(input_files[0])
    pending, pending_rows, total_rows = [], 0, 0

    def flush(writer):
        nonlocal pending, pending_rows
        if pending_rows:
            # One write_table call per row group, so the row groups have the default size across file boundaries.
            writer.write_table(pa.Table.from_batches(pending, schema=schema))
            pending, pending_rows = [], 0

    # ParquetWriter without extra options uses pyarrow's defaults (Snappy, dictionary encoding, 1 MiB data pages, ...).
    with pq.ParquetWriter(output_file, schema) as writer:
        for i, path in enumerate(input_files, 1):
            parquet_file = pq.ParquetFile(path)
            if not parquet_file.schema_arrow.equals(schema):
                sys.exit(f'Schema of {path} differs from that of {input_files[0]}:\n'
                         f'{parquet_file.schema_arrow}\nexpected:\n{schema}')
            print(f'[{i}/{len(input_files)}] {path} ({parquet_file.metadata.num_rows:,} rows)', flush=True)
            for batch in parquet_file.iter_batches(batch_size=READ_BATCH_SIZE):
                # Split at the row group boundary, so no row group exceeds ROW_GROUP_SIZE.
                while batch.num_rows:
                    take = min(batch.num_rows, ROW_GROUP_SIZE - pending_rows)
                    pending.append(batch.slice(0, take))
                    pending_rows += take
                    total_rows += take
                    batch = batch.slice(take)
                    if pending_rows == ROW_GROUP_SIZE:
                        flush(writer)
            parquet_file.close()
        flush(writer)

    print(f'Wrote {total_rows:,} rows from {len(input_files)} files to {output_file}')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('dataset_path', help='directory with the Parquet files to merge')
    parser.add_argument('dataset_name', help='name of the merged file, without .parquet')
    parser.add_argument('save_path', help='directory to write <dataset_name>.parquet to')
    args = parser.parse_args()
    merge(args.dataset_path, args.dataset_name, args.save_path)


if __name__ == '__main__':
    main()
