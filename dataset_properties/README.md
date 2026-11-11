
# Overview

This directory provides scripts for evaluating datasets properties to help us understand why a different Parquet value encoding gives the smallest files for each dataset:
- BYTE_STREAM_SPLIT (BSS) for REDD;
- RLE-dictionary for BLUED (1 MiB dictionary page limit), and for GREEND and 2D-Wind (10 KiB limit).

It also explains why the 10 KiB dictionary page limit beats 1 MiB for GREEND and 2D-Wind.

## Files in this folder and granularity of property evaluation

| File | What it is |
|---|---|
| [data_property_analysis.ipynb](data_property_analysis.ipynb) | The analysis: sizes per column from the experiment databases, the property summary, the encoding × compressor table, figures and findings. |
| [data_properties.py](data_properties.py) | Computes the properties of every row group and the real Parquet size of each column chunk per encoding and compressor. `load()` reads the cached CSV files, and `load(recompute=True)` or `python -I data_properties.py` computes them again. |
| [data_properties.csv](data_properties.csv) | Granularity: one row per column chunk (float and timestamp columns). |
| [data_properties_columns.csv](data_properties_columns.csv) | Granularity: the properties aggregated per column. |
| [data_properties_datasets.csv](data_properties_datasets.csv) | Granularity: the properties aggregated per dataset. |
| [dataset_property_table.md](dataset_property_table.md) / [.tex](dataset_property_table.tex) | The summary table. The LaTeX version is a `table*` with booktabs and tabularx, `\glss{}` dataset names and the label `table:dataset_properties`. |

## Inputs

- **Original datasets:** read from `<repo>/data/<dataset>/*.parquet`, with `<dataset>` one of `blued`, `greend`, `redd` and `neon_wind`. See [Datasets](../README.md#datasets) in the top-level README for where to get them. They are opened read-only.
- **Experiment results:** the notebook reads the SQLite databases that [test_parameters_in_isolation.py](../experiments/runner/test_parameters_in_isolation.py) writes to `<repo>/data/output/<dataset>.db`. It opens them read-only and uses only the rows with `error_bound = '0'`. Two tables are used:
  - `dataset_struct_properties`: one row per file, which is one configuration;
  - `file_properties`: one row per column chunk.

The cached CSV files are included, so the notebook runs without the original datasets. Only recomputing the properties needs them.

## Running

From this folder, with the Python environment from [Python packages](../README.md#python-packages) activated:

```bash
python -I data_properties.py          # recompute the cached CSV files (multiprocessing, about 10 minutes with 24 processes)
jupyter notebook data_property_analysis.ipynb
```

`WORKERS` in [data_properties.py](data_properties.py) sets the number of processes. `SAMPLE_EVERY` sets which row groups of each dataset are measured: 1 means every row group, and 4 means every 4th.

## Method

1. **Sizes from the experiments:** the compressed size per column chunk for four value encodings:
   - PLAIN;
   - BSS;
   - dictionary with a 10 KiB (DICT10K) or 1 MiB (DICT1M) page limit.

   ZSTD-13 has BSS and both dictionary limits. ZSTD-22 has PLAIN. Every other parameter is held fixed:
   - 1,048,576 rows per row group;
   - no statistics;
   - 1 MiB data pages;
   - no bloom filter and no offset index;
   - DELTA_BINARY_PACKED timestamps.

   Dictionary combined with BSS is not a valid configuration. For BLUED, only the first 20 row groups are queried, because its database is large.
2. **Properties of the original data:** computed per row group, with row groups re-cut at 1,048,576 rows in file order, as in the experiments. Properties are computed on non-null values.
3. **Controlled rewrite:** every float column chunk is written as a real Parquet column chunk with pyarrow, nulls included.
   - It uses 4 encodings × 5 compressors: Snappy, LZ4, gzip-9, zstd-13 and Brotli-7.
   - The writer defaults match the experiments: 1 MiB data pages capped at 20,000 rows, and statistics off.
   - The `total_compressed_size` of each written chunk is recorded.
   - Timestamps are not rewritten. pyarrow falls back to PLAIN after a dictionary overflow, while the experiments' writer (parquet-rs) falls back to DELTA_BINARY_PACKED. Timestamp sizes therefore come from the databases (step 1).

The rewritten sizes match the databases. For REDD at zstd-13, a row group with BSS is 3.38 MB rewritten and 3.27 MB in the database; with PLAIN it is 3.96 and 3.90 MB.

### Properties (columns in `data_properties.csv`)

- `distinct` is the number of distinct values in the row group. `cardinality_ratio` is distinct values divided by non-null values.
- `repeat_ratio` is the share of values equal to the previous non-null value, which is what the encoder sees.
- `value_entropy` is the order-0 entropy of the values.
- `H_b0_sign_exp`, `H_b1_exp_mant`, `H_b2_mant` and `H_b3_mant_lsb` are the order-0 entropy of each byte of the float.
  - The bytes are in big-endian order.
  - b0 is the sign and 7 exponent bits.
  - b3 is the least significant mantissa byte.
- `H_b3_given_upper3` and `H_low2_given_high2` are conditional entropies, computed as H(value) − H(value >> 8) and H(value) − H(value >> 16).
- `dict_fits_10K` / `dict_fits_1M` say whether the dictionary fits the page limit: 10,240 or 1,048,576 bytes, at 4 bytes per float and 8 per timestamp. `dict_row_coverage_10K` / `dict_row_coverage_1M` are the share of rows encoded before the dictionary overflows.
- Timestamps also have `cardinality_ratio` and `modal_delta_share`. `modal_delta_share` is the share of deltas equal to the most common step.
- `size_<compressor>_<encoding>` is the real Parquet column chunk size in bytes.

H(low 2 bytes | high 2 bytes) = H(X) − H(X_hi), where X_hi is the top 16 bits of the float32 bit pattern; its range is 0–16 bits. H(X) can't exceed log2(1,048,576) = 20 bits per row group, so the estimate is a lower bound when almost all values are distinct. For REDD it is 19.9 − 10.7 = 9.2 bits. The true value is probably close to 16 bits, since b2 and b3 have 8.00 bits each.

## Findings

[dataset_property_table.md](dataset_property_table.md) summarizes the properties per dataset. The main conclusions:

1. **The best encoding is the same for all 5 compressors.** Only the margin changes.
   - PLAIN is 1.6–2.2× larger than the dictionary with Snappy or LZ4, but only 1.05–1.35× with zstd, gzip or Brotli.
   - For GREEND with Brotli-7, PLAIN beats the dictionary in 16 % of column chunks. GREEND's smallest file overall is PLAIN with Brotli-11: 373.6 MB, against 379.1 MB for DICT10K.
2. **BSS wins only when the low bytes are noise that the high bytes can't predict.**
   - The best predictor is H(low 2 bytes | high 2 bytes). Its Spearman correlation with the BSS gain is 0.73, against 0.50 for the entropy of the lowest byte alone.
   - High entropy in the lowest byte isn't enough. BLUED has 7.7 bits there, yet BSS is 1.9× larger, because with about 1,000 distinct values the high bytes determine the low ones.
   - At 2 bits or less, BSS is 0–185 % larger than PLAIN. At about 4 bits (BLUED `voltage`), BSS ≈ PLAIN. At 9.2 bits or more (REDD), BSS is 10–16 % smaller with every compressor.
3. **Why BSS works on REDD** (zstd-13 per byte stream, on a sample, in bits per value):
   - b2 and b3 cost 8.00 bits each in either layout, because noise can't be compressed.
   - With BSS, the exponent byte b0 changes rarely and becomes long runs: 1.99 bits instead of 8. b1 compresses to 6.94 bits.
   - With PLAIN, every 4-byte window contains noise, so LZ finds no matches and b0 and b1 stay expensive. The result is 30.0 bits per value with PLAIN against 24.9 with BSS.
   - In BLUED, 2D-Wind and GREEND, BSS also makes b0 cheap (0.2–1.2 bits). But it separates b2 and b3 from b0 and b1, which predicted them because whole values repeat. Each then costs about 7 bits, more than the gain.
4. **Dictionary against PLAIN:** the dictionary wins when a row group has a moderate number of distinct values (hundreds to about 11,000) that change on almost every row. LZ finds few matches there, while a dictionary index takes about 10–14 bits.
5. **Dictionary page limit for float columns:** DICT10K and DICT1M give identical bytes when a column chunk has 2,560 or fewer distinct values.
   - BLUED `voltage` has about 11,000 distinct values: 10 KiB overflows, so 1 MiB wins (3.06 against 4.34 GB at zstd-13, on a sample).
   - REDD has more than 262,144 distinct values: both limits overflow, and 1 MiB is 13–14 % larger.
6. **Dictionary page limit for timestamps** (the real reason 10 KiB wins for GREEND and 2D-Wind):
   - The dictionary is enabled or disabled for the whole file, so it also covers the timestamp column, which overflows any dictionary.
   - In every row group, the writer falls back to DELTA_BINARY_PACKED after about 2,048 rows with 10 KiB. With 1 MiB it falls back only after 131,072 rows (a 1 MiB dictionary page plus 17-bit indices).
   - That costs about 0.3–1 MB extra per row group. The cost is large relative to regular timestamps, which DELTA_BINARY_PACKED shrinks to almost nothing (BLUED, 2D-Wind). It is small relative to irregular ones (GREEND, +12 %).
   - For GREEND, the whole gap between the 10 KiB and 1 MiB files (389.0 against 423.3 MB at zstd-13) is the timestamp column (290.3 against 324.6 MB). All 30 float columns are identical.
   - 2D-Wind's timestamps repeat, with a cardinality ratio of 0.31: each site has several measurement series with the same `device` value. The 1 MiB dictionary therefore covers about 40 % of rows before it overflows, which gives the largest cost per row group, about 1.07 MB.
7. **Rules:**
   - Use BSS if H(low 2 bytes | high 2 bytes) is high, and the dictionary otherwise.
   - Use the smallest dictionary page limit that holds the distinct values of the value columns per row group, because the timestamp column always overflows. A dictionary setting per column would remove this trade-off.
   - Weaker compressors widen the dictionary's lead over PLAIN.

### Nulls (GREEND)

Parquet stores nulls as definition levels: 1 bit per row, RLE or bit-packed. With data page v1 they are compressed together with the values. Only non-null values reach the value encoding. GREEND's null ratio of 0.22 in the summary table leaves out column chunks that are entirely null. Over the whole dataset, GREEND is about 71 % null.

## Caveats

- **Data pages:** Parquet writers (arrow-cpp and parquet-rs) cap data pages at 20,000 rows by default, so the experiment files have 52 pages per 1,048,576-row column chunk. Benchmarks with raw zstd on 1 MiB pages make PLAIN look too good, for example on BLUED `voltage`. Real Parquet writes show BSS ≈ PLAIN there.
- **2D-Wind database:** `file_properties` stores every column chunk twice, so the notebook de-duplicates it with `select distinct`.
- **Timestamp encoding:** `timestamp_encoding` varies in the databases (PLAIN or DELTA_BINARY_PACKED) and must be held fixed when comparing value encodings. For example, GREEND DICT1M at zstd-13 is 476.5 MB with PLAIN timestamps and 423.5 MB with DELTA_BINARY_PACKED.
