# PME Implementation Plan — Winter Hackathon 2026

## Context: Data Security at Scale with PME

Sensitive datasets (PII/PCI) must remain encrypted, but analytics teams need role-based access to cleartext values. Today the choice is between security and performance. PME gives both — encrypted by default, decrypted by permission.

This is a greenfield hackathon project (3 engineers). The deliverable:

- **Encrypt at write time**: Lambda (container image) runs PyArrow PME to write column-level encrypted Parquet to S3.
- **Keys managed by AWS KMS**: Each column group gets its own CMK; IAM enforces who can decrypt.
- **Decrypt at read time via QuickSight**: Athena SQL View → Lambda Federated Connector → decrypts PME in RAM → zero storage → QuickSight Direct Query dashboard.

## Architecture

### Write Path (Lambda Container Image)

```
S3 event or manual invoke
  → Lambda (container image with PyArrow)
  → reads CSV/data from S3
  → PyArrow PME Writer encrypts columns with per-column KMS keys
  → writes PME-encrypted Parquet back to S3
```

- KMS wraps DEKs per column group (footer key, PCI key, PII key).
- Only sensitive columns encrypted (`ssn`, `pan` = encrypted; `amount`, `date` = plaintext).
- Lambda container image avoids the 250 MB zip layer limit (PyArrow is ~150 MB).
- Lambda execution role has `kms:Encrypt` + `kms:GenerateDataKey` on all 3 CMKs.

### Read Path: QuickSight via Athena Federated Query

**Important nuance:** Athena SQL (Trino) does not natively support PME decryption — it cannot pass PME column-keys or footer-keys into its internal Parquet reader. Therefore, for QuickSight to "Direct Query" PME data on the fly, you must use a Lambda Federated Connector as a decryption proxy that presents the data to Athena SQL in a way it can understand.

```
S3 (PME-Encrypted Parquet)
  → Lambda Connector (Decrypts in RAM)
  → Athena SQL View
  → QuickSight (Direct Query)
```

#### Step 1: The "Semantic" Bridge — Glue Data Catalog

You don't store the decrypted files, but you **do** store the metadata (the table definition) in the Glue Data Catalog.

- Register the PME-encrypted data as a table in the Glue Catalog, pointing to the S3 path.
- **The problem:** If QuickSight queries this table via Athena SQL, the query will fail because Athena SQL doesn't know how to handle the Parquet encryption footers.

#### Step 2: The "Decryption Proxy" View

To make this work "on the fly" for QuickSight, use a Lambda-based Federated Query:

1. **Register the table in Glue Catalog:** Define the PME-encrypted data as a Glue table pointing to the S3 path.
2. **Deploy a Lambda Federated Connector:** Athena has a Federated Query feature. Deploy a Lambda function (AWS provides templates) that uses the Hadoop PME libraries to read PME data and decrypt it in memory.
3. **Create the Athena SQL view:**
   ```sql
   CREATE VIEW "decrypted_pme_view" AS
   SELECT * FROM "lambda_connector"."database"."pme_table";
   ```
4. **Connect QuickSight:** QuickSight queries the view. When the view is triggered, the Lambda connector uses the KMS keys to decrypt the Parquet blocks in memory and passes the results back to Athena, then to QuickSight.

#### Why this fits the "No Storage" requirement

- **In-Memory Decryption:** The data is decrypted in the Lambda's RAM (the "on the fly" part).
- **No Intermediate Files:** No unencrypted `.parquet` or `.csv` files are ever written to S3.
- **Transient Access:** Once the QuickSight dashboard finishes loading, the decrypted data is purged from the Lambda's memory.

#### Security Implementation

| Component | Requirement |
|-----------|------------|
| KMS Policy | Must allow the `AthenaFederationLambdaRole` to `kms:Decrypt` |
| QuickSight | Connects via Direct Query to the Athena View |
| S3 | Files remain PME-encrypted at rest |

### RBAC Result

| Role | SSN | PAN | Email | Phone | Amount | Date |
|------|-----|-----|-------|-------|--------|------|
| Fraud Analyst | Visible | Visible | Visible | Visible | Visible | Visible |
| Marketing Analyst | NULL | NULL | Visible | Visible | Visible | Visible |
| Junior Analyst | NULL | NULL | NULL | NULL | Visible | Visible |

RBAC is controlled by the caller's IAM role. The single Lambda connector receives the caller's identity via `FederatedIdentity`, assumes the caller's IAM role via STS, and uses that role's KMS grants to decrypt. Columns whose KMS key the caller cannot access are returned as NULL.

### PME Support by Compute Option

| Approach | PME Support | How |
|----------|:-----------:|-----|
| **QuickSight via Federated Query** | **Yes** | Lambda connector decrypts in RAM, Athena SQL view bridges to QuickSight |
| Lambda container image | Yes | PyArrow native C++ PME |
| Athena Spark | Yes | Spark's native JVM Parquet crypto (`parquet.crypto.factory.class`) |
| Athena SQL (Trino) | No | Managed Athena SQL doesn't expose PME key config |
| Snowflake native Parquet | No | Snowflake's reader doesn't support PME |

## Project Structure

```
├── pme/
│   ├── src/
│   │   ├── kms_client.py         # AwsKmsClient — bridges PyArrow → AWS KMS
│   │   ├── encryption.py         # Write pipeline: encrypt columns → PME Parquet → S3
│   │   ├── config.py             # Column→key mappings, KMS ARNs, S3 paths
│   │   └── __init__.py
│   ├── tests/
│   │   ├── conftest.py           # InMemoryKmsClient fixture (no AWS needed)
│   │   ├── test_kms_client.py
│   │   ├── test_encryption.py
│   │   ├── test_integration.py   # Integration tests with real AWS KMS
│   │   └── test_roundtrip.py
│   ├── demo_encrypt.py           # CLI demo: encrypt sample data to S3
│   ├── inspect_encrypted.py      # CLI tool: inspect PME Parquet file structure
│   ├── requirements.txt
│   └── __init__.py
├── lambda/
│   ├── Dockerfile                # Container image: Python 3.12 + PyArrow + s3fs
│   ├── handler.py                # Lambda: reads CSV from S3, encrypts, writes PME Parquet
│   ├── deploy.sh                 # Build, push to ECR, update Lambda function
│   └── requirements.txt          # Lambda-only deps (pyarrow, s3fs — boto3 pre-installed)
├── infra/
│   ├── main.tf                   # Provider + S3 backend
│   ├── variables.tf              # Input variables
│   ├── kms.tf                    # 3 KMS CMKs (footer, PCI, PII)
│   ├── iam.tf                    # RBAC roles + Lambda/connector execution roles
│   ├── s3.tf                     # S3 bucket for encrypted data
│   ├── lambda.tf                 # ECR + Lambda encrypt function + IAM execution role
│   ├── federated.tf              # Athena Federated Connector Lambda (decryption proxy)
│   ├── glue.tf                   # Glue Data Catalog table (PME metadata)
│   ├── quicksight.tf             # QuickSight data source + Athena SQL view
│   └── outputs.tf
├── data/
│   ├── Hackathon_customer_data.csv   # Synthetic sample data (100 rows)
│   └── Hackathon_customer_data_2.csv
├── docs/
│   ├── PME_IMPLEMENTATION_PLAN.md
│   ├── ATHENA_DECRYPTION_GUIDE.md
│   └── SPARK_PME_CHALLENGE.md
└── README.md
```

## Progress Summary

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Project skeleton + environment | DONE |
| Phase 2 | Core KMS client | DONE |
| Phase 3 | Encrypt (write pipeline) | DONE |
| Phase 4 | Sample data | DONE |
| Phase 5 | AWS infrastructure (Terraform) | DONE |
| Phase 6 | Lambda container image — encrypt | DONE |
| Phase 7 | Glue Data Catalog — register PME table metadata | TODO |
| Phase 8 | QuickSight read path — Federated Connector + RBAC | TODO |
| Phase 9 | Benchmarks + polish | TODO |

## Implementation Phases

### DAY 1: Write Path + Lambda Encrypt + QuickSight Read Path

#### Phase 1: Project Skeleton + Environment — DONE

- Create directory structure.
- `pyproject.toml` with package metadata.
- `requirements.txt`: `pyarrow>=15.0`, `boto3>=1.34`, `s3fs>=2024.2`, `pytest>=8.0`, `pandas>=2.1`.
- Verify PyArrow encryption module loads.
- `PmeConfig` dataclass with KMS ARNs, column→key maps, algorithm settings.

#### Phase 2: Core KMS Client (most critical component) — DONE

`AwsKmsClient(pe.KmsClient)` with `wrap_key`/`unwrap_key` backed by boto3 KMS.

**What the KMS client does:**

The KMS client is the bridge between PyArrow's encryption engine and AWS KMS. It has exactly two methods:

- `wrap_key(key_bytes, master_key_id)` → calls KMS encrypt, returns wrapped DEK
- `unwrap_key(wrapped_key, master_key_id)` → calls KMS decrypt, returns plaintext DEK

**What the KMS client does NOT do:**

- It does NOT encrypt/decrypt the actual column data (PyArrow does that with AES-GCM)
- It does NOT decide who has access (IAM policies on the CMK control that)

Additional components:

- `AwsKmsClientFactory` callable for `CryptoFactory`.
- STS assume-role support for RBAC.
- `InMemoryKmsClient` test fixture (no AWS calls needed).
- Unit tests: wrap/unwrap roundtrip.

**Design decisions:**

- `master_key_identifier` = full KMS ARN.
- Wrapped key bytes are base64-encoded (PyArrow stores as UTF-8 in Parquet footer).
- `custom_kms_conf` carries region + optional `role_arn`.
- Single wrapping (`double_wrapping=False`) for hackathon simplicity.

#### Phase 3: Encrypt (Write Pipeline) — DONE

- `write_encrypted_parquet(table, path, config)` → `CryptoFactory` + `EncryptionConfiguration` + `pq.ParquetWriter`.
- `write_encrypted_to_s3(table, config)` → S3 write via `s3fs`.
- Manual partition loop (`write_to_dataset` does NOT support `encryption_properties`).
- Verify encrypted file magic bytes = `PARE`.

**Encryption flow (what happens when PyArrow encrypts a column):**

1. PyArrow generates a random DEK (256-bit AES key) for each column group
2. `KmsClient.wrap_key(DEK, "arn:aws:kms:.../alias/pme-pci-key")` → sends DEK to AWS KMS
3. KMS encrypts the DEK using the CMK (master key) → returns wrapped DEK
4. PyArrow encrypts the column data locally with the plaintext DEK (AES-GCM)
5. Wrapped DEK is stored in the Parquet file footer
6. Plaintext DEK is discarded → never persisted

**Encryption settings:**

- `plaintext_footer=True` (allows schema discovery without keys).
- `encryption_algorithm=AES_GCM_V1` (configurable to `AES_GCM_CTR_V1`).
- `internal_key_material=True` (wrapped DEKs in Parquet footer).
- `cache_lifetime=10 minutes` (reduces KMS API calls).

#### Phase 4: Sample Data — DONE

Sample data is provided in `Hackathon_customer_data.csv` (columns: `first_name`, `last_name`, `ssn`, `email`, `xid`, `balance`). No data generation step needed. CSV is uploaded to `s3://pwe-hackathon-pme-data-651767347247/raw-data/`.

#### Phase 5: AWS Infrastructure (Terraform) — DONE

All infrastructure is deployed in `us-east-2` (account `651767347247`):

- 3 KMS CMKs: `alias/pme-hackathon-footer-key`, `alias/pme-hackathon-pci-key`, `alias/pme-hackathon-pii-key`.
- 4 IAM RBAC Roles: `pme-hackathon-fraud-analyst`, `pme-hackathon-marketing-analyst`, `pme-hackathon-junior-analyst`, `pme-hackathon-write-role`.
- 1 Lambda execution role: `pme-hackathon-lambda-pme-encrypt` (KMS encrypt/decrypt + S3 read/write + CloudWatch logs).
- 1 S3 Bucket: `pwe-hackathon-pme-data-651767347247` with `pme-data/` prefix.
- 1 ECR Repository: `pme-hackathon-pme-lambda`.
- 1 Lambda Function (container image): `pme-hackathon-pme-encrypt` (1536 MB, 300s timeout).
- 1 CloudWatch log group: `/aws/lambda/pme-hackathon-pme-encrypt` (7-day retention).

**Remaining infra (for read path) to be added:**
  - Lambda Federated Connector (decryption proxy for Athena SQL).
  - Glue Data Catalog table (PME metadata registration).
  - QuickSight data source + Athena SQL view.
  - IAM roles for the federated connector with tier-specific `kms:Decrypt` grants.

#### Phase 6: Lambda Container Image — Encrypt — DONE

**Container image (deployed to ECR):**

- Base: `public.ecr.aws/lambda/python:3.12`
- Dependencies: `pyarrow>=15.0`, `s3fs>=2024.2` (boto3 pre-installed in Lambda runtime)
- Copies `pme/` package preserving import structure (`from pme.src.xxx`)
- Handler: `handler.lambda_handler`
- Build: `docker build --platform linux/amd64 --provenance=false` (required for Lambda manifest compat)
- Deploy: `lambda/deploy.sh` handles ECR login, build, push, and `update-function-code`

**Encrypt Lambda (`lambda/handler.py`) — DEPLOYED AND TESTED:**

- Reads config from environment variables (`S3_BUCKET`, `S3_PREFIX`, `FOOTER_KEY_ARN`, `PCI_KEY_ARN`, `PII_KEY_ARN`)
- Accepts event: `{ "input_s3_key": "...", "output_filename": "...", "s3_prefix": "..." }` (all optional with defaults)
- Downloads CSV from S3 via `boto3.client("s3").get_object()`
- Parses CSV with `csv.DictReader` → `pa.Table.from_pylist()`
- Creates `AwsKmsClientFactory` (uses Lambda execution role via default boto3 chain — no `role_arn` needed)
- Calls `write_encrypted_to_s3()` from `pme/src/encryption.py`
- Returns `{ statusCode: 200, body: { output_s3_uri, rows_encrypted, columns_encrypted, input_s3_key } }`

**Test results:**

```
$ aws lambda invoke --function-name pme-hackathon-pme-encrypt --payload '{}' /dev/stdout
{"statusCode": 200, "body": {"output_s3_uri": "s3://pwe-hackathon-pme-data-651767347247/pme-data/customer_data_encrypted.parquet", "rows_encrypted": 100, "columns_encrypted": ["ssn", "first_name", "last_name", "email"]}}
```

#### Phase 7: Glue Data Catalog — Register PME Table Metadata

- Create a Glue database and table definition pointing to the PME-encrypted S3 path.
- Table metadata only — no decrypted files stored. Encrypted Parquet stays in S3 untouched.
- Schema matches the CSV columns: `first_name`, `last_name`, `ssn`, `email`, `xid`, `balance`.
- Terraform resource in `glue.tf`.

#### Phase 8: Lambda Federated Connector — Decryption Proxy

**Important nuance:** Athena SQL (Trino) does not natively support PME decryption — it cannot pass PME column-keys or footer-keys into its internal Parquet reader. Therefore, for QuickSight to "Direct Query" PME data on the fly, you must use a Lambda Federated Connector as a decryption proxy that presents the data to Athena SQL in a way it can understand.

**The architecture to achieve this:**

##### 1. The "Semantic" Bridge: Glue Data Catalog

You don't store the decrypted files, but you **do** store the metadata (the table definition) in the Glue Data Catalog.

- Register an external table in the Glue Data Catalog pointing to the PME-encrypted S3 path (done in Phase 7 via Terraform).
- **The problem:** If QuickSight queries this table via Athena SQL, the query will fail because Athena SQL doesn't know how to handle the Parquet encryption footers.

##### 2. The Solution: The "Decryption Proxy" View

To make this work "on the fly" for QuickSight, use a Lambda-based Federated Query:

1. **Register the Glue table:** The Glue Data Catalog table (created in Phase 7 via Terraform) points to the PME-encrypted S3 path.
2. **Deploy a Lambda Federated Connector:** Athena has a Federated Query feature. Deploy a Lambda function (AWS provides templates) that uses the Hadoop PME libraries to read the PME data and decrypt it in memory.
3. **Create the Athena SQL view:**
   ```sql
   CREATE VIEW "decrypted_pme_view" AS
   SELECT * FROM "lambda_connector"."database"."pme_table";
   ```
4. **Connect QuickSight:** QuickSight queries the view. When the view is triggered, the Lambda connector uses the KMS keys to decrypt the Parquet blocks in memory and passes the results back to Athena, then to QuickSight.

##### Why this fits the "No Storage" requirement

- **In-Memory Decryption:** The data is decrypted in the Lambda's RAM (the "on the fly" part).
- **No Intermediate Files:** No unencrypted `.parquet` or `.csv` files are ever written to S3.
- **Transient Access:** Once the QuickSight dashboard finishes loading, the decrypted data is purged from the Lambda's memory.

##### Security Implementation

| Component | Requirement |
|-----------|------------|
| KMS Policy | Must allow the `AthenaFederationLambdaRole` to `kms:Decrypt` |
| QuickSight | Connects via Direct Query to the Athena View |
| S3 | Files remain PME-encrypted at rest |

##### Data Flow Summary

```
User / QuickSight
  → SELECT * FROM "pme_decrypted_view"
  → Athena invokes single Lambda Connector
  → Connector reads FederatedIdentity (caller's IAM ARN)
  → sts:AssumeRole → caller's analyst role
  → kms:Decrypt with assumed role → allowed columns decrypted, denied = NULL
  → Results returned to Athena → caller
```

##### RBAC Implementation — Single Connector, Caller-Identity Based

Deploy **1 Lambda Federated Connector** and **1 Athena SQL view**. The connector inspects the caller's identity and assumes their IAM role to call KMS — the caller's KMS grants determine which columns are decrypted.

**How it works:**

1. User (or QuickSight) queries `SELECT * FROM "pme_decrypted_view"`.
2. Athena invokes the single Lambda connector.
3. The connector receives `FederatedIdentity` containing the caller's IAM principal ARN.
4. The connector calls `sts:AssumeRole` on the caller's analyst role.
5. Using the assumed role's credentials, the connector calls `kms:Decrypt` for each column key.
6. Columns whose KMS key the assumed role cannot access → returned as NULL.

**IAM Roles & KMS Grants (unchanged):**

| IAM Role | Footer Key | PCI Key | PII Key | Column Visibility |
|----------|:----------:|:-------:|:-------:|-------------------|
| `pme-hackathon-fraud-analyst` | Decrypt | Decrypt | Decrypt | All columns visible |
| `pme-hackathon-marketing-analyst` | Decrypt | DENY | Decrypt | PCI = NULL |
| `pme-hackathon-junior-analyst` | Decrypt | DENY | DENY | PCI + PII = NULL |

**Single view — all roles query the same view:**

```sql
CREATE VIEW "pme_decrypted_view" AS
SELECT * FROM "pme_connector"."pme_db"."customer_data";
```

Same encrypted file on S3, same connector, same view — the caller's IAM role determines which columns are visible.

**Connector IAM policy:** The connector's own execution role needs `sts:AssumeRole` on the 3 analyst roles. Each analyst role's KMS key policy controls the actual decrypt access.

**Terraform:** `federated.tf` for the single connector Lambda + IAM execution role + `sts:AssumeRole` trust. `quicksight.tf` for QuickSight data source and view.

##### QuickSight Configuration

- Data source: Athena (pointed at the workgroup with the federated connector).
- Dataset: Points to the single `pme_decrypted_view`.
- Dataset mode: **Direct Query** (not SPICE) — decryption happens on-the-fly on every dashboard load.
- QuickSight user/group is mapped to the appropriate IAM analyst role — the connector assumes that role when the dashboard loads.
- To demo RBAC: log in as different QuickSight users (each mapped to a different analyst role) and show the same dashboard returning different column visibility.
- Document results for presentation.

### DAY 2: Benchmarks + Polish

#### Phase 9: Benchmarks + Polish

- Encrypted vs. unencrypted write/read at 10K/100K/1M rows.
- `AES_GCM_V1` vs. `AES_GCM_CTR_V1`.
- Lambda connector cold start vs. warm invoke latency.
- QuickSight dashboard load time with PME decryption.
- Benchmark notebook with charts.
- README with architecture diagram.

## Verification Plan

| Check | Command / Method | Status |
|-------|-----------------|--------|
| PyArrow encryption loads | `python -c 'from pyarrow.parquet.encryption import CryptoFactory'` | DONE |
| Unit tests pass (no AWS) | `pytest pme/tests/ -m 'not integration'` | DONE |
| File is encrypted | First 4 bytes = `PAR1` (plaintext footer, columns encrypted) | DONE |
| Roundtrip integrity | encrypt → decrypt → `assert table.equals(original)` | DONE |
| RBAC works locally | Assume each role → verify column visibility | DONE |
| Lambda encrypt works | `aws lambda invoke --function-name pme-hackathon-pme-encrypt --payload '{}'` → 100 rows encrypted to S3 | DONE |
| Glue table registered | Table visible in Glue Data Catalog pointing to S3 PME path | TODO |
| Federated connector deployed | Lambda connector can read and decrypt PME files in memory | TODO |
| Athena SQL view works | `SELECT * FROM "pme_decrypted_view"` returns decrypted rows | TODO |
| RBAC — fraud analyst | Query view as fraud role → all columns visible | TODO |
| RBAC — marketing analyst | Query view as marketing role → PCI = NULL | TODO |
| RBAC — junior analyst | Query view as junior role → PCI + PII = NULL | TODO |
| QuickSight Direct Query | Dashboard loads with decrypted data on-the-fly | TODO |
| Integration tests | `pytest pme/tests/ -m integration` | DONE |

## Commit Strategy (Conventional Commits)

### Day 1

```
feat: add PME project skeleton with pyproject.toml and requirements
feat: implement AwsKmsClient wrapping AWS KMS for PyArrow PME
feat: implement encrypted Parquet write pipeline
feat: add Terraform for KMS keys, IAM roles, S3, ECR, Lambda
feat: add Lambda container image for encrypt handler
feat: add Glue Data Catalog table for PME metadata
feat: add Lambda Federated Connectors with RBAC + QuickSight integration
test: add unit and integration tests for PME roundtrip
```

### Day 2

```
feat: add benchmark notebook comparing encrypted vs unencrypted
docs: add README with architecture diagram and demo instructions
```
