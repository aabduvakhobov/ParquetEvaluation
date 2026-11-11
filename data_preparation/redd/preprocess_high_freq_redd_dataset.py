import sys
import glob
import os
import subprocess

import pandas as pd
import numpy as np
import pyarrow
import pyarrow.parquet as parquet


STDOUT = subprocess.PIPE
STDERR = subprocess.PIPE


def decompress_archive_files(file):
    process = subprocess.run(
        ["tar", "-xjvf", file],
        stdout=STDOUT,
        stderr=STDERR,
    )
    return process.returncode == 0
    

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


def flatten_df(df):
    df = df.rename(columns={0:'time'})
    final_df = pd.DataFrame()
    
    for time in df.time.unique():
        vals = df.loc[df['time'] == time, 2:].values.flatten()
        new_df = pd.DataFrame(vals, columns=["waveform_values_amps"])
        new_df['time'] = time
        final_df = pd.concat([final_df, new_df])
         
    return final_df.reset_index(drop=True)


def add_timestamps_create_table(df, waveform_len=275):
    unique_timestamps = df.time.unique()
    # used for approximating the last timestamp value for the next 275 values
    approximated_si = 0
    new_timestamps = np.array([])
    unique_timestamps_len = len(unique_timestamps)
    for i in range(unique_timestamps_len):
        if i == unique_timestamps_len-1:
            new_timestamps = np.concatenate([
                new_timestamps, 
                np.linspace(start=unique_timestamps[i],
                            stop=unique_timestamps[i]+int(approximated_si/unique_timestamps_len),
                            num=waveform_len,
                            endpoint=False)])
        else:
            approximated_si += unique_timestamps[i+1] - unique_timestamps[i]
            new_timestamps = np.concatenate([
                new_timestamps,
                np.linspace(start=unique_timestamps[i], stop=unique_timestamps[i+1], num=waveform_len, endpoint=False)
                ]) 
    df['time'] = new_timestamps
    df['time'] = (df['time'] * 10**3).astype(int)
    df = pyarrow.Table.from_pandas(df[['time', 'waveform_values_amps']])
    return comply_schema_with_mdb(df)


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
            columns.append(pyarrow.field(safe_name, pyarrow.timestamp("ms")))
        elif field.type in [ pyarrow.float16(), pyarrow.float64(), pyarrow.float32() ]:
            columns.append(pyarrow.field(safe_name, pyarrow.float32()))
        else:
            columns.append(pyarrow.field(safe_name, pyarrow.string()))
    return pyarrow.Table.from_arrays(table.columns, schema = pyarrow.schema(columns))


def main(path, save_path):
    # read the path to RED-RawData
    # unzip the file
    if not decompress_archive_files(path):
        raise ValueError("Failed to untar the archive.")
    
    # list the files in the unzipped directory
    files = get_files(path.replace(".tar.bz2", ""), "dat")
    print(f"Finished extraction and got: {len(files)} files" )
    for file in files:
        df = pd.read_table(file, sep="\s+", header=None)
        # apply transformation script for each .dat file and store as parquet
        df = flatten_df(df)
        df = add_timestamps_create_table(df)
        file_name =  file.split(os.sep)[-2] + "_" + file.split(os.sep)[-1].split(".")[0] + '.parquet'
        
        parquet.write_table(df, save_path + os.sep + file_name)
        print(f"Saved: {save_path + os.sep + file_name}")


if __name__ == "__main__":
    # exit if parameters are not as expected
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} /path/to/file.tar.bz2 /path/to/save/files")
        sys.exit(1)
        
    main(sys.argv[1], sys.argv[2])
 
