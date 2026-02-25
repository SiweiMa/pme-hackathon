# PME Implementation Plan — Winter Hackathon 2026

## Context: Data Security at Scale with PME

Sensitive datasets (PII/PCI) must remain encrypted, but analytics teams need role-based access to cleartext values. Today the choice is between security and performance. PME gives both — encrypted by default, decrypted by permission.

This is a greenfield hackathon project (3 engineers). The deliverable:

- **Encrypt at write time**: Lambda (container image) runs PyArrow PME to write column-level encrypted Parquet to S3.
- **Keys managed by AWS KMS**: Each column group gets its own CMK; IAM enforces who can decrypt.
- **Decrypt at read time via Lambda**: Lambda reads PME Parquet from S3, decrypts on-the-fly based on its execution role's KMS grants; unauthorized columns return NULL.
- **Query with standard SQL (stretch goal)**: Snowflake External Function calls Lambda for role-based decryption.

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

### Read Path (Lambda Container Image)

```
Direct invoke / API Gateway / Snowflake External Function
  → Lambda (container image, one per RBAC tier)
  → PyArrow + CryptoFactory + AwsKmsClient
  → reads PME Parquet from S3
  → IAM execution role determines which KMS keys Lambda can decrypt
  → returns decrypted rows (JSON)
```

**RBAC Result:**

| Role | SSN | PAN | Email | Phone | Amount | Date |
|------|-----|-----|-------|-------|--------|------|
| Fraud Analyst | Visible | Visible | Visible | Visible | Visible | Visible |
| Marketing Analyst | NULL | NULL | Visible | Visible | Visible | Visible |
| Junior Analyst | NULL | NULL | NULL | NULL | Visible | Visible |

### Why Lambda Container Image?

PyArrow Parquet Modular Encryption (PME) is a C++ feature in PyArrow's native Parquet reader/writer. It is **not** available through Spark's JVM-based Parquet handling. This rules out Athena Spark and EMR Spark as compute options — PySpark's `spark.read.parquet()` cannot trigger PME decryption.

| Approach | PME Support | Why |
|----------|:-----------:|-----|
| Athena Spark (PySpark) | No | JVM Parquet reader, no PME support |
| EMR Spark (PySpark) | No | Same JVM reader limitation |
| Snowflake native Parquet | No | Snowflake's reader doesn't support PME |
| **Lambda container image** | **Yes** | Full PyArrow with native C++ PME |
| Glue Python Shell | Yes | PyArrow available, but heavier setup |
| AWS Batch (Fargate) | Yes | Full Docker, but more infra overhead |

**Lambda advantages:**

1. Serverless — no infra to manage, near-zero cost for occasional runs.
2. Container image — install any Python library (PyArrow ~150 MB).
3. Native IAM/KMS — execution role gets KMS access directly, no credential files.
4. Triggerable — S3 events, EventBridge schedule, API Gateway, or manual invoke.
5. 15 min timeout / 10 GB RAM — more than enough for our dataset size.

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
│   ├── iam.tf                    # RBAC roles (fraud/marketing/junior/write)
│   ├── s3.tf                     # S3 bucket for encrypted data
│   ├── lambda.tf                 # ECR + Lambda encrypt function + IAM execution role
│   ├── athena.tf                 # Athena Spark workgroups
│   └── outputs.tf
├── Hackathon_customer_data.csv   # Synthetic sample data (100 rows)
└── README.md
```

## Progress Summary

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | Project skeleton + environment | DONE |
| Phase 2 | Core KMS client | DONE |
| Phase 3 | Encrypt (write pipeline) | DONE |
| Phase 4 | Decrypt (read pipeline) + RBAC | TODO |
| Phase 5 | Sample data | DONE |
| Phase 6 | AWS infrastructure (Terraform) | DONE |
| Phase 7 | Lambda container image — encrypt | DONE |
| Phase 8 | RBAC demo + verification | TODO |
| Phase 9 | API Gateway | TODO |
| Phase 10 | Snowflake integration | TODO |
| Phase 11 | Benchmarks + polish | TODO |

## Implementation Phases

### DAY 1: Write Path + Lambda Encrypt/Decrypt + RBAC Demo

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

#### Phase 4: Decrypt (Read Pipeline) + RBAC

- `read_encrypted_parquet(path, config, role_arn=None)`.
- Graceful degradation: failed decryption → null arrays for those columns.

**Decryption flow (what happens when PyArrow reads an encrypted column):**

1. PyArrow reads the Parquet footer → finds wrapped DEK for the column group
2. `KmsClient.unwrap_key(wrapped_DEK, "arn:aws:kms:.../alias/pme-pci-key")` → sends to KMS
3. If IAM role has `kms:Decrypt` on that CMK → KMS returns plaintext DEK → PyArrow decrypts column
4. If IAM role lacks `kms:Decrypt` → KMS returns `AccessDenied` → PyArrow returns NULL for that column

> This is how RBAC works: the IAM role attached to the Lambda determines which KMS keys it can unwrap, which determines which columns are visible.

- RBAC integration tests against the 3-role matrix.
- Full roundtrip: generate → encrypt → decrypt → compare.

**RBAC matrix (3 KMS keys × 3 IAM roles):**

| Role | Footer Key | PCI Key | PII Key | Visibility |
|------|-----------|---------|---------|------------|
| `pme-fraud-analyst` | Decrypt | Decrypt | Decrypt | Sees ALL |
| `pme-marketing-analyst` | Decrypt | DENY | Decrypt | Sees PII, not PCI |
| `pme-junior-analyst` | Decrypt | DENY | DENY | Sees non-sensitive only |

#### Phase 5: Sample Data — DONE

Sample data is provided in `Hackathon_customer_data.csv` (columns: `first_name`, `last_name`, `ssn`, `email`, `xid`, `balance`). No data generation step needed. CSV is uploaded to `s3://pwe-hackathon-pme-data-651767347247/raw-data/`.

#### Phase 6: AWS Infrastructure (Terraform) — DONE

All infrastructure is deployed in `us-east-2` (account `651767347247`):

- 3 KMS CMKs: `alias/pwe-hackathon-footer-key`, `alias/pwe-hackathon-pci-key`, `alias/pwe-hackathon-pii-key`.
- 4 IAM RBAC Roles: `pwe-hackathon-fraud-analyst`, `pwe-hackathon-marketing-analyst`, `pwe-hackathon-junior-analyst`, `pwe-hackathon-write-role`.
- 1 Lambda execution role: `pwe-hackathon-lambda-pme-encrypt` (KMS encrypt/decrypt + S3 read/write + CloudWatch logs).
- 1 S3 Bucket: `pwe-hackathon-pme-data-651767347247` with `pme-data/` prefix.
- 1 ECR Repository: `pwe-hackathon-pme-lambda`.
- 1 Lambda Function (container image): `pwe-hackathon-pme-encrypt` (1536 MB, 300s timeout).
- 1 CloudWatch log group: `/aws/lambda/pwe-hackathon-pme-encrypt` (7-day retention).
- 3 Athena Spark workgroups: fraud, marketing, junior analyst.

**Remaining Lambda functions (decrypt) to be added:**
  - `pwe-hackathon-decrypt-fraud`: fraud analyst role — decrypts all columns.
  - `pwe-hackathon-decrypt-marketing`: marketing role — decrypts PII only.
  - `pwe-hackathon-decrypt-junior`: junior role — decrypts non-sensitive only.

**Why IAM roles are critical:**

```
KMS CMKs
  → IAM execution roles with different kms:Decrypt grants
  → Lambda functions bound to those roles
  → Same PME file, different column visibility per Lambda
```

Remove IAM roles → every Lambda has the same KMS access → no RBAC demo.

#### Phase 7: Lambda Container Image — Encrypt — DONE

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
$ aws lambda invoke --function-name pwe-hackathon-pme-encrypt --payload '{}' /dev/stdout
{"statusCode": 200, "body": {"output_s3_uri": "s3://pwe-hackathon-pme-data-651767347247/pme-data/customer_data_encrypted.parquet", "rows_encrypted": 100, "columns_encrypted": ["ssn", "first_name", "last_name", "email"]}}
```

**Decrypt Lambda — TODO:**

- Separate handler for reading PME Parquet and decrypting based on execution role
- One Lambda per RBAC tier (fraud/marketing/junior), each with different IAM execution role
- Returns JSON rows; unauthorized columns return NULL

**RBAC demo flow (planned):**

```
Invoke pwe-hackathon-decrypt-fraud   → all columns visible
Invoke pwe-hackathon-decrypt-marketing → PCI columns = NULL
Invoke pwe-hackathon-decrypt-junior   → PCI + PII = NULL
```

Same encrypted file, same code, different IAM role → different column visibility.

#### Phase 8: RBAC Demo + Verification

- Invoke all 3 decrypt Lambdas against the same PME file.
- Verify column visibility matches the RBAC matrix.
- Compare output side-by-side to demonstrate the access control.
- Document results for presentation.

### DAY 2: Snowflake Integration + Benchmarks + Polish

#### Phase 9: API Gateway

- 1 REST API with 3 resource paths: `/decrypt-fraud`, `/decrypt-marketing`, `/decrypt-junior`.
- Each path integrates with its corresponding Lambda function.
- API Gateway invoke permissions for Snowflake's IAM principal.

#### Phase 10: Snowflake Integration

- Create API Integration pointing to API Gateway.
- Create 3 External Functions (one per RBAC tier).
- Create 3 Snowflake roles mapped to the external functions.
- Grant each role access to only its external function.
- Create wrapper view that auto-selects function based on `CURRENT_ROLE()`.
- Demo queries: same table, different roles, different column visibility.

**Example Snowflake SQL:**

```sql
-- fraud analyst sees everything
USE ROLE pme_fraud_analyst;
SELECT t.value:transaction_id::STRING, t.value:ssn::STRING, t.value:amount::FLOAT
FROM TABLE(RESULT_SCAN(pme_decrypt_fraud('pme-data/data.parquet'))) t;

-- junior analyst sees NULLs for sensitive columns
USE ROLE pme_junior_analyst;
SELECT t.value:transaction_id::STRING, t.value:ssn::STRING, t.value:amount::FLOAT
FROM TABLE(RESULT_SCAN(pme_decrypt_junior('pme-data/data.parquet'))) t;
```

#### Phase 11: Benchmarks + Polish

- Encrypted vs. unencrypted write/read at 10K/100K/1M rows.
- `AES_GCM_V1` vs. `AES_GCM_CTR_V1`.
- Lambda cold start vs. warm invoke latency.
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
| Lambda encrypt works | `aws lambda invoke --function-name pwe-hackathon-pme-encrypt --payload '{}'` → 100 rows encrypted to S3 | DONE |
| Lambda decrypt — fraud | Invoke → all columns have values | TODO |
| Lambda decrypt — marketing | Invoke → PCI columns = NULL | TODO |
| Lambda decrypt — junior | Invoke → PCI + PII columns = NULL | TODO |
| Snowflake RBAC demo | Same query as 3 roles → different columns visible | TODO |
| Integration tests | `pytest pme/tests/ -m integration` | DONE |

## Commit Strategy (Conventional Commits)

### Day 1

```
feat: add PME project skeleton with pyproject.toml and requirements
feat: implement AwsKmsClient wrapping AWS KMS for PyArrow PME
feat: implement encrypted Parquet write pipeline
feat: implement encrypted Parquet read pipeline with RBAC degradation
feat: add Terraform for KMS keys, IAM roles, S3, ECR, Lambda
feat: add Lambda container image with encrypt and decrypt handlers
feat: add RBAC demo invoking 3 decrypt Lambdas side-by-side
test: add unit and integration tests for PME roundtrip
```

### Day 2

```
feat: add API Gateway with per-role decrypt endpoints
feat: add Snowflake setup SQL with API integration and external functions
feat: add benchmark notebook comparing encrypted vs unencrypted
docs: add README with architecture diagram and demo instructions
```
