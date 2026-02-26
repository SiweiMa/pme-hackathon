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

## Is the Glue catalog table needed for federated queries?

No. When you query through the federated connector (`"pme-hackathon-pme-connector"."pme-hackathon-pme-db"."customer_data"`), **the Lambda handler is entirely self-contained** and never calls the Glue API. It hardcodes:

- The schema name (`SCHEMA_NAME = "pme-hackathon-pme-db"`)
- The table name (`TABLE_NAME = "customer_data"`)
- The Arrow schema (6 columns in `TABLE_SCHEMA`)
- The S3 location (from `S3_BUCKET` / `S3_PREFIX` env vars)
- The KMS key ARNs (from `FOOTER_KEY_ARN` / `PCI_KEY_ARN` / `PII_KEY_ARN` env vars)

The Glue catalog table in `infra/glue.tf` (with its `parameters`, `storage_descriptor`, and `ser_de_info`) is **not consulted** during federated queries. It would only matter if someone queried through the default `AwsDataCatalog`, which would fail anyway since Athena cannot natively decrypt PME-encrypted Parquet.

The Glue resources can be kept for documentation purposes but are not functionally required.

## How does `customer_data` show up in the Athena console sidebar?

When you select **"pme-hackathon-pme-connector"** as the data source in the Athena console, the UI invokes the Lambda three times to populate the sidebar:

### 1. ListSchemasRequest — discover databases

Athena asks: "What databases does this data source have?"

The Lambda responds with `["pme-hackathon-pme-db"]` (`handler.py:168–170`). The UI renders this as a database node.

### 2. ListTablesRequest — discover tables

You expand the database. Athena asks: "What tables are in this database?"

The Lambda responds with `[{"schemaName": "pme-hackathon-pme-db", "tableName": "customer_data"}]` (`handler.py:173–175`). The UI renders the table node.

### 3. GetTableRequest — discover columns

You expand the table. Athena asks: "What columns does this table have?"

The Lambda responds with the Arrow schema containing 6 fields (`handler.py:178–180`). The schema is serialized via `federation.py` (Arrow flatbuffer → strip 4-byte prefix → Base64). The UI renders the column list with types.

### Result in the sidebar

```
Data source: pme-hackathon-pme-connector    ← Terraform aws_athena_data_catalog
  └── Database: pme-hackathon-pme-db        ← Lambda ListSchemasRequest
        └── Table: customer_data            ← Lambda ListTablesRequest
              ├── first_name  (string)      ← Lambda GetTableRequest
              ├── last_name   (string)
              ├── ssn         (string)
              ├── email       (string)
              ├── xid         (string)
              └── balance     (double)
```

The only thing from AWS infrastructure is the top-level data source name (the `aws_athena_data_catalog` resource). Everything below it comes from the Lambda's hardcoded responses.

## What happens when you run a federated query end-to-end?

Running:

```sql
SELECT * FROM "pme-hackathon-pme-connector"."pme-hackathon-pme-db"."customer_data" LIMIT 10;
```

triggers the following sequence of Lambda invocations:

### Step 1: PingRequest

Athena health-checks the connector. Lambda returns `PingResponse` with `sourceType: "pme_connector"` (`handler.py:162–165`).

### Step 2: GetTableRequest

Athena asks for the table schema. Lambda returns the 6-column Arrow schema (`handler.py:178–180`). Athena uses this to plan the query.

### Step 3: GetTableLayoutRequest

Athena asks for partition information. Lambda returns a single partition `{partition_id: 0}` since the dataset is small and not partitioned (`handler.py:183–188`).

### Step 4: GetSplitsRequest

Athena asks "what chunks of data should I read?" Lambda lists S3 objects under `s3://{S3_BUCKET}/{S3_PREFIX}/`, filters for `.parquet` files containing `"customer_data"`, and returns one split per file with the S3 key in `properties.s3_key` (`handler.py:191–214`).

### Step 5: ReadRecordsRequest (core decryption + RBAC)

Athena sends one request per split. This is where decryption and access control happen (`handler.py:217–269`):

**5a. Extract caller identity and determine RBAC restrictions**

The caller's IAM ARN is extracted from `event["identity"]["arn"]`. The `rbac.py` module matches it against a static role map:

| Caller role contains | Denied KMS keys | Columns visible |
|---|---|---|
| `pme-hackathon-fraud-analyst` | _(none)_ | All 6 columns |
| `pme-hackathon-marketing-analyst` | `pci-key` | All except `ssn` |
| `pme-hackathon-junior-analyst` | `pci-key`, `pii-key` | Only `xid`, `balance` |
| _(unknown caller)_ | `pci-key`, `pii-key` | Only `xid`, `balance` |

**5b. Download encrypted Parquet from S3**

The S3 key from the split properties is downloaded to Lambda's `/tmp` ephemeral storage (`handler.py:238`).

**5c. Build PME config and KMS client factory**

`_build_config()` creates a `PmeConfig` with three KMS key groups (`handler.py:83–112`):
- **Footer key** — decrypts Parquet footer metadata
- **PCI group** — `ssn` column, encrypted with `pme-hackathon-pci-key`
- **PII group** — `first_name`, `last_name`, `email`, encrypted with `pme-hackathon-pii-key`

**5d. Full decryption**

`read_with_partial_access()` (`decryption.py:134–205`) first performs a full-access read:
1. PyArrow reads the Parquet footer and finds wrapped DEKs (data encryption keys)
2. For each column group, PyArrow calls `KmsClient.unwrap_key(wrapped_DEK, key_alias)` → AWS KMS `Decrypt` API returns the plaintext DEK
3. PyArrow decrypts column data locally using AES-GCM with the plaintext DEK
4. Returns a fully decrypted `pa.Table`

**5e. RBAC null-masking**

For each column group whose KMS key alias is in `denied_key_aliases`, the column is replaced with `pa.nulls(num_rows)`. For example, a junior analyst gets `first_name`, `last_name`, `ssn`, and `email` all replaced with nulls.

**5f. Serialize and return**

The masked table is serialized to Arrow IPC format (RecordBatch → strip 4-byte prefix → Base64) and returned as a `ReadRecordsResponse` (`federation.py:186–208`).

**5g. Cleanup**

The temp file in `/tmp` is deleted in the `finally` block (`handler.py:267–269`).

### Step 6: Athena renders results

Athena deserializes the Arrow IPC batch, applies `LIMIT 10`, and displays the result. What you see depends on your IAM role:

| Role | first_name | last_name | ssn | email | xid | balance |
|---|---|---|---|---|---|---|
| Fraud analyst | Alice | Smith | 123-45-6789 | alice@ex.com | XID001 | 1500.00 |
| Marketing analyst | Alice | Smith | _null_ | alice@ex.com | XID001 | 1500.00 |
| Junior analyst | _null_ | _null_ | _null_ | _null_ | XID001 | 1500.00 |
