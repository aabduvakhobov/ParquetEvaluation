import sys
import glob
import os
import bz2

import pandas as pd
import numpy as np
import pyarrow
import pyarrow.parquet as parquet


# helper functions
def get_files(path, file_format):
    # Reuses a method from ModelatData/Utilities: https://github.com/ModelarData/Utilities/blob/main/Apache-Parquet-Loader/main.py
    if os.path.isdir(path):
        full_files = []
        files = glob.glob(path + os.sep + "*." + file_format)
        if len(files) == 0 and os.path.isdir(path + os.sep):
            for dir, _, all_files in os.walk(path + os.sep):
                for file in all_files:
                    if "." + file_format in file:
                        full_files.append(dir + os.sep + file)
            files = full_files
        files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        files = [path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return files


def read_blued_file(file):
    # Step 1: Open and decompress the file
    with bz2.open(file, 'rt') as f:
        lines = f.readlines()
    # Step 2: Find the second '***End_of_Header***'
    header_count = 0
    data_start_index = 0

    time_str = None
    date_str = None

    for i, line in enumerate(lines):
        if line.startswith('Date,') and header_count == 1:
            date_str = line.strip().split(',')[1]
        if line.startswith('Time,') and header_count == 1:
            time_str = line.strip().split(',')[1]
        if '***End_of_Header***' in line:
            header_count += 1
            if header_count == 2:
                data_start_index = i + 1
                break

    # Step 3: Read the data block using pandas
    from io import StringIO

    data_text = ''.join(lines[data_start_index:])
    df = pd.read_csv(StringIO(data_text))
    df['start_date_time'] =  date_str + " " + time_str
    df['start_date_time'] = pd.to_datetime(df['start_date_time'])

    df['start_date_time_int_microseconds'] = df['start_date_time'].astype('int64') // 1000
    # convert xvalue into microseconds 
    df['X_Value'] = df['X_Value'] * 10**6
    df['time'] = (df['start_date_time_int_microseconds'] + df['X_Value']).astype('int64')
    df = df.rename(columns={'Current A':"current_a", "Current B" : "current_b", 'VoltageA' : "voltage"})
    cols_to_keep = ['time', 'current_a', 'current_b', 'voltage']
    return df[cols_to_keep]

def comply_schema_with_mdb(table):
    columns = [] 
    for field in table.schema:
        print(field.name)
        safe_name = field.name.replace(" ", "_")
        if field.type in [
            pyarrow.timestamp("s"),
            pyarrow.timestamp("ms"),
            pyarrow.timestamp("us"),
            pyarrow.timestamp("ns"),
            pyarrow.timestamp("us", tz='UTC'),
            pyarrow.timestamp("ns", tz='UTC')
        ] or 'time' in field.name :
            columns.append(pyarrow.field(safe_name, pyarrow.timestamp("us")))
        elif field.type in [ pyarrow.float16(), pyarrow.float64(), pyarrow.float32() ]:
            columns.append(pyarrow.field(safe_name, pyarrow.float32()))
        else:
            columns.append(pyarrow.field(safe_name, pyarrow.string()))
    return pyarrow.Table.from_arrays(table.columns, schema = pyarrow.schema(columns))


def main(path, save_path):
    # find all files
    files = get_files(path, "txt.bz2")
    
    for file in files:
        filename = file.split('/')[-1].replace('txt.bz2', 'parquet')
        df = read_blued_file(file)
        df = comply_schema_with_mdb(pyarrow.Table.from_pandas(df))
        parquet.write_table(df, save_path + os.sep + filename)
        print(f"saved: {filename}")
        

if __name__ == "__main__":
    # exit if parameters are not as expected
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} /path/to/file.tar.bz2 /path/to/save/files")
        sys.exit(1)
        
    main(sys.argv[1], sys.argv[2])