# PME — Frequently Asked Questions

## Is decrypted data stored anywhere?

No. The decrypted data exists only in Lambda's memory during execution:

1. **Encrypted file downloaded to `/tmp`** — deleted in the `finally` block after decryption.
2. **Decrypted `pa.Table` lives in RAM** — serialized to Arrow IPC — returned to Athena.
3. **Athena processes the Arrow bytes in its own memory** — writes query results to the S3 output location (as CSV, not Parquet).

The query results in S3 (`s3://spill-bucket/query-results/`) do contain plaintext, but that's standard Athena behavior for all queries. You can control this with workgroup settings (encryption at rest, auto-delete).

## How does performance compare with row-level encryption/decryption?

PME (file-level) encryption is significantly faster than row-level encryption because it amortizes cryptographic overhead across the entire file rather than paying it per row:

| Aspect | PME (file-level) | Row-level encryption |
|--------|-------------------|----------------------|
| **Encrypt/Decrypt calls** | 1 per file | 1 per row × number of columns encrypted |
| **KMS round-trips** | 1 `GenerateDataKey` + 1 `Decrypt` per file | 1 per row (or batched, but still O(n)) |
| **Throughput** | Bulk AES-256 on the entire Parquet payload | Per-cell AES operations, each with envelope-key overhead |
| **Latency at query time** | Single decrypt + Arrow deserialization | Row-by-row decrypt during scan; scales linearly with table size |
| **Parquet compatibility** | Standard Parquet (encrypted as an opaque blob) | Requires Parquet Modular Encryption (PME columns) or custom serde |

**Key takeaway:** For a 100K-row file, file-level PME makes ~2 KMS API calls and one bulk AES operation. Row-level encryption on the same file could require thousands of KMS calls (even with data-key caching) and per-cell crypto, adding seconds of latency. The trade-off is granularity — row-level lets you decrypt individual columns/rows, while file-level is all-or-nothing.
