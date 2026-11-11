import sys
import glob
import os
import subprocess

import pandas as pd
import numpy as np
import pyarrow
import pyarrow.parquet as parquet
import duckdb

AIR_PRESSURE_SITES = ["BLAN", "CPER", "OSBS", "JORN", "JERC", "STER", "BART" ]
WIND_SPEED_SITES = ["BLAN", "DSNY", "JERC", "CPER", "STER", "JORN", ]
WIND_SPEED_NEW_SITES = ['ABBY', 'ARIK', 'BARC', 'BART', 'BIGC', 'BLDE', 'BLUE', 'BONA',
                  'CARI', 'CLBJ', 'COMO', 'CRAM', 'CUPE', 'DCFS', 'DEJU', 'DELA',
                   'FLNT', 'GRSM', 'GUAN', 'GUIL', 'HARV', 'HEAL', 'HOPB', 'KING',
                    'KONA', 'KONZ', 'LAJA', 'LECO', 'LENO', 'LEWI', 'LIRO', 'MART',
                     'MAYF', 'MCDI', 'MCRA', 'MLBS', 'MOAB', 'NIWO', 'NOGP', 'OAES',
                      'OKSR', 'ONAQ', 'ORNL', 'OSBS', 'POSE', 'PRIN', 'PRLA', 'PRPO',
                       'PUUM', 'REDB', 'RMNP', 'SCBI', 'SERC', 'SJER', 'SOAP', 'SRER',
                        'STEI', 'STER', 'SUGG', 'SYCA', 'TALL', 'TEAK', 'TECR', 'TOOK',
                         'TOOL', 'TREE', 'UKFS', 'UNDE', 'WALK', 'WLOU', 'WOOD', 'WREF', 'YELL']
COLS_TO_KEEP = {
    "air_pressure": ["endDateTime", "staPresMean", "staPresMinimum", "staPresMaximum"],
    "wind_speed": ["endDateTime", "windSpeedMean", "windSpeedMinimum", "windSpeedMaximum"]
    }
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
        csv_files = glob.glob(path + os.sep + "*" + file_format)
        if len(csv_files) == 0 and os.path.isdir(path + os.sep):
            for dir,_,files in os.walk(path + os.sep):
                for file in files:
                    if "." + file_format in file:
                        full_files.append(dir + os.sep + file)
            csv_files = full_files
        csv_files.sort()  # Makes ingestion order more intuitive.
    elif os.path.isfile(path):
        csv_files = [path]
    else:
        raise ValueError("parquet_file_or_folder is not a file or a folder")
    return csv_files


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
        ]:
            columns.append(pyarrow.field(safe_name, pyarrow.timestamp("ms")))
        elif field.type in [ pyarrow.float16(), pyarrow.float64(), pyarrow.float32() ]:
            columns.append(pyarrow.field(safe_name, pyarrow.float32()))
        else:
            columns.append(pyarrow.field(safe_name, pyarrow.string()))
    return pyarrow.Table.from_arrays(table.columns, schema = pyarrow.schema(columns))


def main(path, save_path, dataset_name):
    # read the path to data
    sites = AIR_PRESSURE_SITES if dataset_name == 'air_pressure' else WIND_SPEED_NEW_SITES
    si = '1min' if dataset_name == 'air_pressure' else "2min"
    # list the files in the unzipped directory
    files = get_files(path, "csv")
    print(f"Finished extraction and got: {len(files)} files" )
    
    for site in sites:
        site_files = [f for f in files if site in f and si in f]
        df = duckdb.read_csv(site_files).to_df()
        df = df[COLS_TO_KEEP[dataset_name]]
        df = df.dropna()
        df = comply_schema_with_mdb(pyarrow.Table.from_pandas(df[COLS_TO_KEEP[dataset_name]]))        
        file_name =  site + '.parquet'
        parquet.write_table(df.select(COLS_TO_KEEP[dataset_name]), save_path + os.sep + file_name)
        print(f"Saved: {save_path + os.sep + file_name}")


if __name__ == "__main__":
    # exit if parameters are not as expected
    if len(sys.argv) != 4 or sys.argv[3] not in ['air_pressure', 'wind_speed']:
        print(f"Usage: {sys.argv[0]} /path/to/neon_root /path/to/save/files dataset_name[air_pressure or wind_speed]")
        sys.exit(1)
        
    main(sys.argv[1], sys.argv[2], sys.argv[3])