# GREEND

Scripts that turn the raw GREEND CSVs (8 Italian/Austrian households, ~1 Hz per-appliance power readings) into Parquet.

| Script | Output |
|---|---|
| `preproces_greend.py` | Long format: `building_id, device, timestamp, measurement` (~975M rows, ~4.3 GB) |
| `greend_to_wide.py` | Multivariate time series: `building_id, timestamp, coffee_machine, …, tv, tv2, …` (~111M rows, 32 columns, ~0.6 GB) |

Requirements: Python 3 with `polars` and `pyarrow` (tested with polars 1.42.1, pyarrow 25.0.1).

```bash
pip install polars pyarrow
```

## 1. Download

Download the dataset archive `GREEND_0-1_311014.zip` (~1.1 GB) from the project page:
<https://sourceforge.net/projects/greend/>

```bash
mkdir -p ~/data/GREEND
mv ~/Downloads/GREEND_0-1_311014.zip ~/data/GREEND/
```

## 2. Unzip

```bash
cd ~/data/GREEND
unzip GREEND_0-1_311014.zip
```

This creates `GREEND_0-1_311014/` (~9.8 GB) with one folder per building, each holding one CSV per day:

```
GREEND_0-1_311014/
├── building0/dataset_2013-12-07.csv
├── building0/dataset_2013-12-08.csv
├── ...
└── building7/...
```

## 3. Long format: `preproces_greend.py`

```bash
python preproces_greend.py ~/data/GREEND/GREEND_0-1_311014 ~/data/GREEND/greend.parquet
```

- Device IDs are mapped to the appliance names from the GREEND paper (`greend_appliances.py`).
  Devices that are 100% null or not documented in the paper are dropped.
- Exact duplicate records are removed; NULL measurements are kept.
- Rows are sorted by `building_id, device, timestamp`, one building per row group.

## 4. Wide format: `greend_to_wide.py`

```bash
python greend_to_wide.py ~/data/GREEND/greend.parquet ~/data/GREEND/greend_wide.parquet
```

- One column per appliance type, shared by all buildings: every building's TV is in `tv`.
  A second device of the same type in a building goes into `tv2`, a third into `tv3`, in the
  column order of `greend_appliances.DEVICES`. The script prints the device → column mapping per building.
- Each building's devices are joined on their exact original timestamps (no rounding or resampling).
  A device without a reading at a timestamp, or one the building does not have, is NULL.
- Rows are sorted by `building_id, timestamp`.
- One building is processed at a time: peak memory is ~13 GB, runtime ~2 min.

The experiments read GREEND from `<repo>/data/greend/`, so move the result there:

```bash
mkdir -p data/greend && mv ~/data/GREEND/greend_wide.parquet data/greend/
```

Note: the wide format cannot distinguish a NULL measurement in the source from a missing reading.

## Helper scripts

- `greend_appliances.py`: device ID → appliance name/type mapping per building (used by both scripts above).
- `greend_verify_columns.py`: profiles every device (power distribution, daily usage pattern) to check which
  appliance each column holds across buildings; `python greend_verify_columns.py ~/data/GREEND/GREEND_0-1_311014 [out.json]`.
- `greend_check_timestamps.py`: checks timestamp uniqueness/order per building, sampling interval and overlap
  across buildings; `python greend_check_timestamps.py ~/data/GREEND/GREEND_0-1_311014`.
