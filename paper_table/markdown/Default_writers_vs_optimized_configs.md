| Target | Dataset | Encoding (timestamp, value) | Block compressor | Statistics |
|:---:|:---:|:---:|:---:|:---:|
| **Low Ingestion Time** | BLUED | Delta-Binary-Packed, Byte-Stream-Split | LZ4 | None |
| | GREEND | Delta-Binary-Packed, Plain | Uncompressed | None |
| | REDD, NEON Wind | Plain, Plain | Uncompressed | None |
| **Low Storage Size** | BLUED | Delta-Binary-Packed, RLE-Dictionary | Brotli(11) | None |
| | GREEND | Delta-Binary-Packed, Plain | Brotli(11) | None |
| | REDD | Delta-Binary-Packed, Byte-Stream-Split | Brotli(11) | None |
| | NEON Wind | Delta-Binary-Packed, RLE-Dictionary(10240) | Brotli(11) | None |
| **Low Query Time** | BLUED, REDD, NEON Wind | Plain, Plain | Uncompressed | Page |
| | GREEND | Delta-Binary-Packed, Plain | Uncompressed | Chunk |
| **Balanced** | BLUED | Delta-Binary-Packed, RLE-Dictionary | LZ4 | Chunk |
| | GREEND | Delta-Binary-Packed, RLE-Dictionary(10240) | Zstd(1) | Page |
| | REDD | Delta-Binary-Packed, Byte-Stream-Split | LZ4 | Page |
| | NEON Wind | Delta-Binary-Packed, RLE-Dictionary(10240) | LZ4 | Page |

*Optimal configurations for each target and dataset. Used in Section IV for RQ1*
