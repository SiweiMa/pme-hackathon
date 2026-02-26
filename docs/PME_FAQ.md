# PME — Frequently Asked Questions

## Is decrypted data stored anywhere?

No. The decrypted data exists only in Lambda's memory during execution:

1. **Encrypted file downloaded to `/tmp`** — deleted in the `finally` block after decryption.
2. **Decrypted `pa.Table` lives in RAM** — serialized to Arrow IPC — returned to Athena.
3. **Athena processes the Arrow bytes in its own memory** — writes query results to the S3 output location (as CSV, not Parquet).

The query results in S3 (`s3://spill-bucket/query-results/`) do contain plaintext, but that's standard Athena behavior for all queries. You can control this with workgroup settings (encryption at rest, auto-delete).
