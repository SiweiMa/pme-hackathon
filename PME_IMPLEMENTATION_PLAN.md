# PME Implementation Plan — Winter Hackathon 2026

## Context: Data Security at Scale with PME

Sensitive datasets (PII/PCI) must remain encrypted, but analytics teams need role-based access to cleartext values. Today the choice is between security and performance. PME gives both — encrypted by default, decrypted by permission.

This is a greenfield hackathon project (3 engineers). The deliverable:

- **Encrypt at write time**: PyArrow writes PME Parquet to S3 with column-level encryption.
- **Keys managed by AWS KMS**: Each column group gets its own CMK; IAM enforces who can decrypt.
- **Decrypt at read time via Snowflake**: External Function calls Lambda that uses PyArrow + KMS to decrypt on-the-fly; unauthorized users see NULL/masked columns.
- **Query with standard SQL**: Snowflake queries return decrypted or masked data based on role.

## Architecture

### Write Path

```
Synthetic Data → PyArrow PME Writer → S3 (PME-Encrypted Parquet)
```

- KMS wraps DEKs per column group (footer key, PCI key, PII key).
- Only sensitive columns encrypted (`ssn`, `pan` = encrypted; `amount`, `date` = plaintext).

### Read Path — Day 2 (Snowflake + Lambda)

```
Snowflake SQL query
  → External Function (one per RBAC tier)
  → API Gateway
  → Lambda
  → Lambda reads PME file from S3 with PyArrow + KMS decryption
  → IAM role determines which KMS keys Lambda can use
  → Returns decrypted rows to Snowflake
```

**RBAC Result:**

| Role | SSN | PAN | Email | Phone | Amount | Date |
|------|-----|-----|-------|-------|--------|------|
| Fraud Analyst | ✅ Visible | ✅ Visible | ✅ Visible | ✅ Visible | ✅ Visible | ✅ Visible |
| Marketing Analyst | ❌ NULL | ❌ NULL | ✅ Visible | ✅ Visible | ✅ Visible | ✅ Visible |
| Junior Analyst | ❌ NULL | ❌ NULL | ❌ NULL | ❌ NULL | ✅ Visible | ✅ Visible |

**Why External Function + Lambda?**

Snowflake cannot read PME-encrypted Parquet natively. PME decryption is embedded in the Parquet reader, and Snowflake's reader doesn't support it.

| Approach | PME Support | Persistence | Complexity |
|----------|-------------|-------------|------------|
| Snowflake native Parquet | ❌ No PME support | N/A | N/A |
| External Function + Lambda | ✅ PyArrow decrypts | Zero (on-the-fly) | Medium |
| Athena Spark (Day 1) | ✅ PyArrow decrypts | Temporary (Glue table) | Low |

### Read Path — Day 1 (Athena Spark + QuickSight)

```
Athena Spark Notebook
  → PyArrow + CryptoFactory + AwsKmsClient
  → reads PME Parquet from S3
  → IAM role (via STS assume-role) determines KMS key access
  → decrypted DataFrame
  → Glue table (SSE-KMS, 1-day lifecycle)
  → QuickSight visualization
```

**Why Athena Spark for Day 1:**

- No Lambda function needed — PyArrow runs natively in Athena Spark
- No API Gateway to configure
- Same decrypt code as local (reuses `src/decryption.py`)
- QuickSight connects directly to Athena for visualization

**Trade-off:** Athena Spark materializes decrypted data to Glue tables (encrypted at rest with SSE-KMS, 1-day lifecycle auto-delete). The Snowflake + Lambda path (Day 2) decrypts on-the-fly with zero persistence.

## Project Structure

```
pme/
├── src/
│   ├── kms_client.py         # AwsKmsClient — bridges PyArrow → AWS KMS
│   ├── encryption.py         # Write pipeline: encrypt columns → PME Parquet → S3
│   ├── decryption.py         # Read pipeline: PyArrow decrypt (used by Lambda)
│   ├── config.py             # Column→key mappings, KMS ARNs, S3 paths
│   └── data_generator.py     # Faker-based synthetic PCI/PII dataset
├── lambda/
│   ├── handler.py            # Lambda: reads PME from S3, decrypts, returns rows
│   ├── requirements.txt      # Lambda dependencies (pyarrow, boto3)
│   └── deploy.sh             # Package + deploy Lambda
├── snowflake/
│   ├── setup.sql             # API integration, external functions, roles, grants
│   └── demo_queries.sql      # Demo queries showing RBAC in action
├── notebooks/
│   ├── 01_generate_and_encrypt.ipynb
│   ├── 02_local_decrypt_rbac.ipynb
│   ├── 03_benchmark.ipynb
│   └── 03_athena_spark_decrypt.ipynb    # Athena Spark PME decrypt
├── athena/
│   └── spark_notebook.py               # Athena Spark PME decrypt script
├── quicksight/
│   └── dashboard_config.json           # QuickSight dashboard config
├── infra/
│   ├── kms.tf                # 3 KMS CMKs (footer, PCI, PII)
│   ├── iam.tf                # RBAC roles + Lambda execution roles
│   ├── s3.tf                 # S3 bucket for encrypted data
│   ├── athena.tf             # Athena Spark workgroup (Day 1)
│   ├── quicksight.tf         # QuickSight resources (Day 1)
│   ├── lambda.tf             # Lambda function + API Gateway
│   └── variables.tf / outputs.tf
├── tests/
│   ├── conftest.py           # InMemoryKmsClient fixture (no AWS needed)
│   ├── test_kms_client.py
│   ├── test_encryption.py
│   ├── test_decryption.py
│   └── test_roundtrip.py
├── requirements.txt
├── pyproject.toml
└── README.md
```

## Implementation Phases

### DAY 1: Write Path + Athena Spark / QuickSight Read

#### Phase 1: Project Skeleton + Environment

- Create directory structure.
- `pyproject.toml` with package metadata.
- `requirements.txt`: `pyarrow>=15.0`, `boto3>=1.34`, `faker>=22.0`, `s3fs>=2024.2`, `pytest>=8.0`, `pandas>=2.1`.
- Verify PyArrow encryption module loads.
- `PmeConfig` dataclass with KMS ARNs, column→key maps, algorithm settings.

#### Phase 2: Core KMS Client (most critical component)

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

#### Phase 3: Encrypt (Write Pipeline)

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

> This is how RBAC works: the IAM role attached to the compute determines which KMS keys it can unwrap, which determines which columns are visible.

- RBAC integration tests against the 3-role matrix.
- Full roundtrip: generate → encrypt → decrypt → compare.

**RBAC matrix (3 KMS keys × 3 IAM roles):**

| Role | Footer Key | PCI Key | PII Key | Visibility |
|------|-----------|---------|---------|------------|
| `pme-fraud-analyst` | Decrypt | Decrypt | Decrypt | Sees ALL |
| `pme-marketing-analyst` | Decrypt | DENY | Decrypt | Sees PII, not PCI |
| `pme-junior-analyst` | Decrypt | DENY | DENY | Sees non-sensitive only |

#### Phase 5: Synthetic Data Generator

- `generate_transactions(n_rows)` using Faker.
- Columns:
  - **PCI**: `ssn`, `pan`, `card_number`
  - **PII**: `first_name`, `last_name`, `email`, `phone`
  - **Non-sensitive**: `transaction_id`, `amount`, `currency`, `transaction_date`, `merchant_name`, `merchant_category`, `status`
- Generate at 10K / 100K / 1M rows for benchmarking.

#### Phase 6a: AWS Infrastructure — Day 1 (Terraform)

- 3 KMS CMKs: `alias/pme-footer-key`, `alias/pme-pci-key`, `alias/pme-pii-key`.
- 3 IAM RBAC Roles: `pme-fraud-analyst`, `pme-marketing-analyst`, `pme-junior-analyst`.
- 1 S3 Bucket: `hackathon-pme-2026` with `pme-data/` prefix.
- Athena Spark workgroup with 3 IAM-role-bound configurations.
- QuickSight data source + dashboard resources.

**Why IAM roles are required on Day 1:**

The entire Day 1 read path demo depends on IAM RBAC roles:

```
KMS CMKs (Phase 6a)
  → IAM RBAC Roles with different kms:Decrypt grants (Phase 6a)
  → Athena Spark workgroups bound to those roles (Phase 7a)
  → Glue tables with different column visibility (Phase 7a)
  → QuickSight dashboard showing the difference (Phase 8a)
```

Remove IAM roles → every workgroup has the same KMS access → no RBAC demo.

**Day 1 IAM resources (required):**

- `pme-fraud-analyst` role → `kms:Decrypt` on all 3 CMKs → sees everything
- `pme-marketing-analyst` role → `kms:Decrypt` on footer + PII keys only → PCI = NULL
- `pme-junior-analyst` role → `kms:Decrypt` on footer key only → PCI + PII = NULL
- Write role (or dev credentials) → `kms:Encrypt` on all 3 CMKs → for encryption pipeline

**Day 2 IAM resources (deferred):**

- 3 Lambda execution roles (one per RBAC tier)
- API Gateway invoke permissions
- Snowflake-related IAM trust policies

#### Phase 7a: Athena Spark PME Decryption (Day 1 Read Path)

- Athena Spark notebook reuses `src/decryption.py` (PyArrow + CryptoFactory + AwsKmsClient).
- 3 Athena Spark workgroups, each bound to a different IAM role:
  - `pme-fraud` workgroup → `pme-fraud-analyst` role → footer + PCI + PII KMS access → all columns decrypted.
  - `pme-marketing` workgroup → `pme-marketing-analyst` role → footer + PII KMS access → PCI = NULL.
  - `pme-junior` workgroup → `pme-junior-analyst` role → footer KMS access only → PCI + PII = NULL.
- Decrypted results written to Glue tables (SSE-KMS encrypted at rest, 1-day S3 lifecycle auto-delete).
- **Trade-off:** materializes decrypted data temporarily. Acceptable for hackathon demo; Snowflake path (Day 2) provides zero-persistence decryption.

#### Phase 8a: QuickSight Dashboard (Day 1 Visualization)

- QuickSight connects to Athena SQL → queries Glue tables produced by Athena Spark.
- Dashboard shows 3 side-by-side panels (one per role):
  - **Fraud Analyst**: all columns visible (SSN, PAN, email, amount, etc.).
  - **Marketing Analyst**: PCI columns = NULL, PII visible.
  - **Junior Analyst**: PCI + PII = NULL, only non-sensitive columns visible.
- Visual RBAC demo: same underlying data, different column visibility per role.

### DAY 2: Snowflake Read Path + Benchmarks + Polish

#### Phase 6b: AWS Infrastructure — Day 2 (Terraform)

- 3 Lambda Functions: one per RBAC tier with different execution roles.
- 1 API Gateway: REST API with 3 resource paths (`/decrypt-fraud`, `/decrypt-marketing`, `/decrypt-junior`).
- 3 Lambda execution IAM roles with tier-specific `KMS:Decrypt` grants.

#### Phase 7b: Lambda Decrypt Function (Day 2 Read Path)

- Lambda handler receives S3 path + requested columns from Snowflake.
- Reads PME Parquet from S3 using PyArrow + CryptoFactory + AwsKmsClient.
- KMS permissions come from Lambda execution role (different per RBAC tier).
- Returns JSON rows to Snowflake via API Gateway response.
- Deploy script: pip install into package dir, zip, upload.

**Lambda data flow:**

```
Snowflake External Function call
  → API Gateway (POST /decrypt-fraud or /decrypt-marketing or /decrypt-junior)
  → Lambda (execution role with specific KMS:Decrypt grants)
  → PyArrow reads PME file from S3, decrypts authorized columns, NULLs the rest
  → Returns JSON array of rows
  → Snowflake receives result set
```

#### Phase 8b: Snowflake Integration

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

#### Phase 9: Benchmarks + Polish

- Encrypted vs. unencrypted write/read at 10K/100K/1M rows.
- `AES_GCM_V1` vs. `AES_GCM_CTR_V1`.
- Athena Spark vs. Snowflake external function latency (end-to-end).
- Benchmark notebook with charts.
- README with Mermaid architecture diagram.

## Verification Plan

| Check | Command / Method |
|-------|-----------------|
| PyArrow encryption loads | `python -c 'from pyarrow.parquet.encryption import CryptoFactory'` |
| Unit tests pass (no AWS) | `pytest pme/tests/ -m 'not integration'` |
| File is encrypted | First 4 bytes = `PARE` (not `PAR1`) |
| Roundtrip integrity | encrypt → decrypt → `assert table.equals(original)` |
| RBAC works locally | Assume each role → verify column visibility |
| Athena Spark RBAC demo | 3 workgroups → 3 different column visibility results |
| QuickSight dashboard | Visual RBAC comparison across roles |
| Lambda decrypts correctly | Invoke Lambda with test event → verify JSON |
| Snowflake RBAC demo | Same query as 3 roles → different columns visible |
| Integration tests | `pytest pme/tests/ -m integration` |

## Commit Strategy (Conventional Commits)

### Day 1

```
feat: add PME project skeleton with pyproject.toml and requirements
feat: implement AwsKmsClient wrapping AWS KMS for PyArrow PME
feat: add synthetic PCI/PII data generator with Faker
feat: implement encrypted Parquet write pipeline
feat: implement encrypted Parquet read pipeline with RBAC degradation
feat: add Terraform for KMS keys, IAM roles, S3, Athena Spark workgroup
feat: add Athena Spark notebook for PME decryption with RBAC
feat: add QuickSight dashboard showing role-based column visibility
test: add unit tests for PME encryption roundtrip
```

### Day 2

```
feat: add Terraform for Lambda functions and API Gateway
feat: add Lambda decrypt handler for Snowflake external function
feat: add Snowflake setup SQL with API integration and external functions
feat: add benchmark notebook comparing encrypted vs unencrypted + Athena vs Snowflake
docs: add README with architecture diagram and demo instructions
```
