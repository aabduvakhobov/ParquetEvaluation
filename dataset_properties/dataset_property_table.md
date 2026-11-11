# Data properties that explain the best Parquet value encoding per dataset

Source: [data_property_analysis.ipynb](data_property_analysis.ipynb) and [data_properties.py](data_properties.py).

- Values are medians over float value column chunks, with row groups are cut at 1,048,576 rows as in the experiments: all row groups of greend, redd and 2d_wind, and every 4th row group of blued (25 %, 1,647 row groups).
- Encoding sizes come from writing each column chunk as a real Parquet column chunk (pyarrow, nulls included, 1 MiB data pages capped at 20,000 rows) for PLAIN, BYTE_STREAM_SPLIT (BSS) and dictionary with a 10 KiB or 1 MiB dictionary page limit, × LZ4, zstd-13 and brotli-7.
- Timestamp sizes come from the experiment result DBs (`error_bound = 0`, ZSTD-13, DELTA_BINARY_PACKED timestamps, other parameters at defaults).
- **Bold** marks the property that sets a dataset apart.

| | **blued** | **greend** | **2d_wind** | **redd** |
|---|---|---|---|---|
| Distinct values per row group | 1,099 (`voltage`: ~11,000) | 97 | 1,040 | **1,015,022** |
| Cardinality ratio (distinct / values) | 0.001 | 0.00012 | 0.001 | **0.97** |
| Repeat ratio (value = previous value) | 0.03 | **0.87** | 0.03 | 0.00 |
| Entropy of byte 1: sign + 7 exponent bits (bits, max 8) | 2.5 | 0.7 | 1.7 | 3.6 |
| Entropy of byte 2: last exponent bit + top 7 mantissa bits (bits, max 8) | 7.4 | 0.9 | 7.3 | 7.7 |
| Entropy of byte 3: middle mantissa bits (bits, max 8) | 7.7 | 0.9 | 4.6 | 8.0 |
| Entropy of lowest mantissa byte (bits, max 8) | 7.7 | 0.9 | 4.6 | 8.0 |
| Entropy of low 2 bytes given high 2 bytes (bits) | 0.3 (`voltage`: 4.0) | 0.0 | 0.2 | **9.2** |
| Column chunks whose dictionary fits 10 KiB (2,560 floats) | **66 %** | 100 % | 98 % | 0 % |
| Column chunks whose dictionary fits 1 MiB (262,144 floats) | 100 % | 100 % | 100 % | **1 %** |
| Null ratio (column chunks with at least one value) | 0 | **0.22** | 0 | 0 |
| Timestamps on the dominant step (share of deltas) | 0.67 | 0.00 | **1.00** | 0.11 |
| Timestamp size at ZSTD-13: no dictionary → 1 MiB dictionary | 0.08 → 9.3 MB (first 20 row groups) | 290 → 325 MB | **1.4 → 205 MB** | 44 → 73 MB |
| **Best value encoding** | **Dictionary, 1 MiB** | **Dictionary, 10 KiB** (≈ PLAIN with strong compressors) | **Dictionary, 10 KiB** | **BSS** |
| Runner-up at zstd-13 (float columns, size vs best) | PLAIN 1.22× | PLAIN 1.14× | PLAIN 1.18× | PLAIN 1.19× |
| BSS at zstd-13 (vs best) | 1.89× | 2.25× | 1.93× | 1.00× |
| Why | Few distinct values that change every row: dictionary indices beat LZ. `voltage` needs more than 10 KiB of dictionary. | Long runs of a few values: dictionary and LZ both catch them. The dictionary fits in 10 KiB, so 1 MiB only adds timestamp overhead. | Same as blued, but the dictionary fits in 10 KiB. The regular timestamps make 1 MiB's overhead very costly. | Values almost never repeat and the low bytes are noise: the dictionary overflows, and BSS isolates the noise. |
