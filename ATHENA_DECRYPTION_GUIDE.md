# Athena Decryption via Lambda Federated Connector

How the PME-encrypted Parquet data gets decrypted and served to Athena SQL queries, with per-role column visibility.

## The Problem

Athena SQL (Trino engine) reads Parquet files directly from S3. It has no support for PyArrow Modular Encryption (PME). If you point Athena at a PME-encrypted Parquet file, you get:

```
HIVE_CURSOR_ERROR: Failed to read Parquet file: s3://bucket/file.parquet
```

The file is encrypted at the column level — the footer contains wrapped Data Encryption Keys (DEKs), and each column's data pages are AES-GCM encrypted. Standard Parquet readers cannot make sense of it.

## The Solution: Lambda Federated Connector

Instead of Athena reading S3 directly, we insert a **Lambda function** between Athena and the data. Athena's [Federated Query](https://docs.aws.amazon.com/athena/latest/ug/connect-to-a-data-source.html) feature allows a Lambda function to act as a custom data source. The Lambda:

1. Receives structured requests from Athena (JSON with `@type` routing)
2. Downloads the encrypted Parquet from S3
3. Decrypts it using PyArrow + AWS KMS
4. Applies RBAC null-masking based on who is querying
5. Serializes the result as Arrow IPC and returns it to Athena

```
User (assumed IAM role)
  │
  ▼
Athena SQL ──── "SELECT * FROM connector.db.table"
  │
  │  (Federation protocol: JSON + Arrow IPC over Lambda invoke)
  ▼
Lambda Connector ──── connector/handler.py
  │
  ├── 1. Extract caller identity (event.identity.arn)
  ├── 2. Map caller → denied KMS key aliases (connector/rbac.py)
  ├── 3. Download PME Parquet from S3 to /tmp
  ├── 4. Decrypt ALL columns via PyArrow + KMS (pme/src/decryption.py)
  ├── 5. Null-mask denied columns (application-level RBAC)
  └── 6. Serialize to Arrow IPC → Base64 → JSON response
  │
  ▼
Athena ──── displays results (some columns may be NULL)
```

## Federation Protocol: The 7-Request Handshake

When you run a `SELECT` query, Athena doesn't just call the Lambda once. It makes **up to 7 sequential Lambda invocations**, each with a different `@type`. The Lambda must respond correctly to all of them.

### Request Flow

```
Athena                              Lambda
  │                                    │
  ├── PingRequest ──────────────────► health check
  │◄── PingResponse ◄─────────────────┤
  │                                    │
  ├── ListSchemasRequest ──────────► what databases?
  │◄── ["pwe-hackathon-pme-db"] ◄─────┤
  │                                    │
  ├── ListTablesRequest ───────────► what tables?
  │◄── [{"schemaName":..,"tableName":"customer_data"}] ◄──┤
  │                                    │
  ├── GetTableRequest ─────────────► what columns?
  │◄── Arrow schema (6 fields) ◄──────┤
  │                                    │
  ├── GetTableLayoutRequest ───────► what partitions?
  │◄── single partition (id=0) ◄──────┤
  │                                    │
  ├── GetSplitsRequest ────────────► what files/chunks?
  │◄── single split (s3_key=...) ◄────┤
  │                                    │
  ├── ReadRecordsRequest ──────────► give me the data!
  │◄── Arrow IPC (decrypted rows) ◄───┤
  │                                    │
  ▼  (query complete)                  │
```

### What Each Request Does

| Request | Lambda Response | Purpose |
|---------|----------------|---------|
| `PingRequest` | capabilities + source type | Health check, connector discovery |
| `ListSchemasRequest` | `["pwe-hackathon-pme-db"]` | Enumerate available databases |
| `ListTablesRequest` | `[{schemaName, tableName}]` | Enumerate tables in a database |
| `GetTableRequest` | Arrow schema (Base64) | Column names and types |
| `GetTableLayoutRequest` | Partition info (Arrow block) | How data is partitioned |
| `GetSplitsRequest` | List of splits with S3 keys | How to parallelize reads |
| `ReadRecordsRequest` | Decrypted Arrow data (Base64) | **The actual data** |

Only `ReadRecordsRequest` triggers decryption. The other 6 are metadata-only.

## Wire Format: Arrow IPC over JSON

The trickiest part of building a Python connector is matching the **exact byte format** that Athena's Java deserializer expects. The official SDK is Java-only; we reverse-engineered the format from the [dacort/athena-federation-python-sdk](https://github.com/dacort/athena-federation-python-sdk).

### Schema Serialization

```python
# NOT the IPC stream format — raw flatbuffer with 4-byte prefix stripped
base64(schema.serialize().slice(4))
```

PyArrow's `schema.serialize()` returns an Arrow flatbuffer with a 4-byte continuation marker prefix. The Java SDK's `SchemaSerDeV3` expects the raw flatbuffer **without** that prefix. So we `slice(4)` to remove it.

In JSON responses, schemas are wrapped in a dict:
```json
{"schema": {"schema": "<base64 flatbuffer>"}}
```

### Record Block Serialization

Record blocks (used for `records` and `partitions`) are also flatbuffers:

```python
batch = table.combine_chunks().to_batches()[0]
{
    "aId": "<uuid>",
    "schema": base64(batch.schema.serialize().slice(4)),
    "records": base64(batch.serialize().slice(4))
}
```

Each block has three fields:
- `aId` — a random UUID (allocator ID, required but value doesn't matter)
- `schema` — the Arrow schema for this block
- `records` — the actual row data as a serialized RecordBatch

### What We Tried That Didn't Work

1. **IPC stream format** (`ipc.new_stream` → `writer.close()`): Athena's deserializer rejected this with `LambdaSerializationException`. The IPC stream format includes a stream header and EOS marker that the Java `SchemaSerDeV3` doesn't expect.

2. **Binary block format** (`[4-byte schema size][schema IPC][batch IPC]`): This is what the Java `BlockSerializer` uses internally, but the JSON serialization wraps it in the `{aId, schema, records}` dict before sending over the wire.

3. **Flat Base64 strings** for schema/records: The Java deserializer expects the nested dict format, not a plain string.

## Decryption: How PME Works Under the Hood

### Encryption Structure (Write Path)

When a Parquet file is PME-encrypted, the structure is:

```
Parquet File
├── Footer (plaintext in our case — contains schema + row group metadata)
│   ├── Wrapped DEK for footer (encrypted by footer KMS key)
│   └── Per-column metadata:
│       ├── column "ssn" → wrapped DEK (encrypted by PCI KMS key)
│       ├── column "first_name" → wrapped DEK (encrypted by PII KMS key)
│       └── ...
├── Row Group 0
│   ├── Column "ssn" data pages → AES-GCM encrypted with ssn's DEK
│   ├── Column "first_name" data pages → AES-GCM encrypted with first_name's DEK
│   ├── Column "xid" data pages → plaintext (not in any encryption group)
│   └── ...
```

Three KMS keys control access:
- **Footer key** (`pwe-hackathon-footer-key`): Required to read any column metadata
- **PCI key** (`pwe-hackathon-pci-key`): Encrypts `ssn`
- **PII key** (`pwe-hackathon-pii-key`): Encrypts `first_name`, `last_name`, `email`

Columns `xid` and `balance` are not encrypted (they're non-sensitive).

### Decryption Flow (Read Path)

```
Lambda downloads PME Parquet from S3 to /tmp
  │
  ▼
PyArrow opens file with FileDecryptionProperties
  │
  ├── Reads footer → finds wrapped DEK for footer
  ├── Calls KMS: Decrypt(wrapped_footer_DEK) → plaintext footer DEK
  ├── Decrypts footer metadata → gets schema + per-column wrapped DEKs
  │
  ├── For each encrypted column needed:
  │   ├── Calls KMS: Decrypt(wrapped_column_DEK) → plaintext column DEK
  │   └── Decrypts column data pages with AES-GCM using that DEK
  │
  └── Returns pa.Table with all columns decrypted
```

The KMS calls happen transparently inside PyArrow's C++ layer via our `AwsKmsClientFactory`. The factory creates `KmsClient` instances that call `kms:Decrypt` on the appropriate KMS key ARN.

**Key detail:** PyArrow's C++ decryption engine decrypts ALL columns in the file, even if you pass `columns=["xid"]`. There's no way to skip decryption of a specific column — either you have the key and everything decrypts, or you get an error. This is why RBAC must be application-level.

### RBAC: Application-Level Null Masking

Since PyArrow must decrypt everything, RBAC is enforced **after** decryption:

```python
# Step 1: Decrypt with full access (Lambda role has all 3 KMS keys)
table = read_with_partial_access(
    local_path, config, factory,
    denied_key_aliases={"pwe-hackathon-pci-key"},  # marketing analyst
)

# Inside read_with_partial_access:
#   1. read_encrypted_parquet() → full decrypt → all columns visible
#   2. Find columns encrypted by denied keys:
#      "pwe-hackathon-pci-key" → ["ssn"]
#   3. Replace those columns with null arrays:
#      table = table.set_column(idx, "ssn", pa.nulls(100, pa.string()))
```

The Lambda's IAM execution role has `kms:Decrypt` on ALL 3 keys. It always decrypts everything. The caller's identity determines which columns get nulled out afterward.

### RBAC Mapping

The caller's IAM ARN is extracted from the Athena federation request's `identity.arn` field:

```python
# From the event payload:
identity = event.get("identity", {})
caller_arn = identity.get("arn", "")
# → "arn:aws:sts::651767347247:assumed-role/pwe-hackathon-fraud-analyst/session"
```

This ARN is matched against a static mapping:

| Caller ARN contains | Denied KMS Keys | Nulled Columns |
|---------------------|-----------------|----------------|
| `pwe-hackathon-fraud-analyst` | (none) | (none) |
| `pwe-hackathon-marketing-analyst` | PCI key | `ssn` |
| `pwe-hackathon-junior-analyst` | PCI + PII keys | `ssn`, `first_name`, `last_name`, `email` |
| (unknown caller) | PCI + PII keys | `ssn`, `first_name`, `last_name`, `email` |

## Request Lifecycle: Full `ReadRecordsRequest` Walkthrough

This is what happens end-to-end when a fraud analyst runs:

```sql
SELECT * FROM "pwe-hackathon-pme-connector"."pwe-hackathon-pme-db"."customer_data" LIMIT 10;
```

### 1. Athena Invokes Lambda

Athena sends a JSON payload to the Lambda:

```json
{
  "@type": "ReadRecordsRequest",
  "catalogName": "pwe-hackathon-pme-connector",
  "identity": {
    "arn": "arn:aws:sts::651767347247:assumed-role/pwe-hackathon-fraud-analyst/session"
  },
  "split": {
    "properties": {
      "s3_key": "pme-data/customer_data_encrypted.parquet"
    },
    "spillLocation": { ... },
    "encryptionKey": { ... }
  },
  "schema": { ... }
}
```

### 2. Handler Routes the Request

`lambda_handler` reads `@type` → dispatches to `_handle_read_records`.

### 3. RBAC Resolution

```python
caller_arn = "arn:aws:sts::651767347247:assumed-role/pwe-hackathon-fraud-analyst/session"
denied_keys = denied_keys_for_caller(caller_arn)
# → frozenset() (fraud analyst has full access)
```

### 4. Download from S3

```python
s3.download_file("pwe-hackathon-pme-data-651767347247",
                  "pme-data/customer_data_encrypted.parquet",
                  "/tmp/tmpXXXXXX.parquet")
```

### 5. Build Config + KMS Factory

```python
config = PmeConfig(
    footer_key=KmsKeyConfig(key_arn="arn:aws:kms:...", alias="pwe-hackathon-footer-key"),
    column_groups=[
        ColumnGroupConfig(name="pci", kms_key=..., columns=["ssn"]),
        ColumnGroupConfig(name="pii", kms_key=..., columns=["first_name", "last_name", "email"]),
    ],
)
factory = AwsKmsClientFactory(region="us-east-2", alias_to_arn={...})
```

### 6. Decrypt + RBAC

```python
table = read_with_partial_access(
    "/tmp/tmpXXXXXX.parquet", config, factory,
    denied_key_aliases=frozenset(),  # fraud analyst: nothing denied
)
# → pa.Table with 100 rows, 6 columns, all fully decrypted
```

### 7. Serialize Response

```python
response = {
    "@type": "ReadRecordsResponse",
    "catalogName": "pwe-hackathon-pme-connector",
    "schema": {"schema": base64(schema.serialize().slice(4))},
    "records": {
        "aId": "a1b2c3d4-...",
        "schema": base64(batch.schema.serialize().slice(4)),
        "records": base64(batch.serialize().slice(4)),
    },
    "requestType": "READ_RECORDS",
}
```

### 8. Athena Receives Results

Athena's Java deserializer reads the JSON, Base64-decodes the Arrow flatbuffers, reconstructs the RecordBatch, and displays the rows in the query results.

## Infrastructure

All infrastructure is defined in Terraform (`infra/federated.tf`):

| Resource | Purpose |
|----------|---------|
| ECR repository | Stores the connector Docker image |
| Lambda function (3 GB, 5 min timeout) | Runs the connector |
| IAM execution role | KMS Decrypt (all 3 keys), S3 read, CloudWatch logs |
| Athena data catalog (LAMBDA type) | Registers the Lambda as a data source |
| S3 spill bucket | Required by federation protocol for large results |
| Athena workgroup | Pre-configured query output location |
| Lambda permission | Allows `athena.amazonaws.com` to invoke |

The connector is deployed as a container image (not a zip) because PyArrow's native C++ libraries are too large for a standard Lambda zip deployment.

## Files

| File | Role |
|------|------|
| `connector/handler.py` | Lambda entry point, request routing, orchestration |
| `connector/federation.py` | Wire protocol serialization (Arrow ↔ Base64 ↔ JSON) |
| `connector/rbac.py` | Caller ARN → denied KMS key aliases mapping |
| `connector/Dockerfile` | Container image build |
| `pme/src/decryption.py` | PyArrow PME decryption + null masking |
| `pme/src/kms_client.py` | AWS KMS client factory for PyArrow CryptoFactory |
| `pme/src/config.py` | PmeConfig, ColumnGroupConfig, KmsKeyConfig dataclasses |
| `infra/federated.tf` | Terraform for all connector infrastructure |
