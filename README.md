# PME — Parquet Modular Encryption Hackathon

## Background

Sensitive datasets containing PII (names, emails, phones) and PCI (SSNs, PANs) must remain encrypted at rest, but analytics teams need role-based access to cleartext values for their work. Traditional approaches force a choice between **security** (full encryption, nobody can query) and **usability** (plaintext, everyone can see everything).

**Parquet Modular Encryption (PME)** eliminates this trade-off. PME encrypts individual columns within a Parquet file using separate keys, so access is controlled at the column level by AWS KMS and IAM policies. The same file serves all roles — what each analyst sees depends on which KMS keys their IAM role can decrypt.

This hackathon project (Winter 2026, 3 engineers) builds a complete PME pipeline:

- **Write path**: PyArrow encrypts sensitive columns with per-column-group KMS keys and writes PME Parquet to S3.
- **Read path (Day 1)**: Athena Spark + QuickSight decrypts on-read using IAM-role-bound workgroups.
- **Read path (Day 2)**: Snowflake External Functions call Lambda for on-the-fly decryption — zero data persistence.

## Progress

| Phase | Description | Status |
|-------|-------------|--------|
| 1. Project Skeleton | `pyproject.toml`, `requirements.txt`, directory structure | :white_check_mark: Done |
| 2. KMS Client | `AwsKmsClient` + `AwsKmsClientFactory` bridging PyArrow to AWS KMS | :white_check_mark: Done |
| 3. Write Pipeline | `write_encrypted_parquet()`, `write_encrypted_to_s3()`, magic byte verification | :white_check_mark: Done |
| 4. Read Pipeline + RBAC | `read_encrypted_parquet()` with graceful degradation for denied columns | :x: Not started |
| 5. Sample Data | `Hackathon_customer_data.csv` with synthetic PII/PCI | :white_check_mark: Done |
| 6a. AWS Infra (Day 1) | KMS CMKs, IAM RBAC roles, S3 bucket, Athena Spark workgroup (Terraform) | :x: Not started |
| 7a. Athena Spark Decrypt | Athena Spark notebook reusing `decryption.py` with 3 IAM-role-bound workgroups | :x: Not started |
| 8a. QuickSight Dashboard | 3-panel dashboard showing column visibility per role | :x: Not started |
| 6b. AWS Infra (Day 2) | Lambda functions, API Gateway, Lambda execution IAM roles (Terraform) | :x: Not started |
| 7b. Lambda Decrypt | Lambda handler for Snowflake external function | :x: Not started |
| 8b. Snowflake Integration | API integration, external functions, RBAC demo queries | :x: Not started |
| 9. Benchmarks | Encrypted vs unencrypted perf at 10K/100K/1M rows | :x: Not started |
| CI | GitHub Actions running unit tests (Python 3.10–3.12) | :white_check_mark: Done |

**Overall: 4 / 12 phases complete**

```
[========>-----------------------] 33%
```

## Architecture

```
WRITE PATH:
  CSV / DataFrame → PyArrow PME Writer → S3 (column-level encrypted Parquet)
                        │
                  KMS wraps DEKs per column group
                  (footer key, PCI key, PII key)

READ PATH (Day 1 — Athena Spark):
  Athena Spark → PyArrow + KMS decrypt → Glue table → QuickSight dashboard

READ PATH (Day 2 — Snowflake):
  Snowflake SQL → External Function → API Gateway → Lambda
                                                      │
                                        PyArrow + KMS decrypt from S3
                                        IAM role controls column access
```

**RBAC model — same file, different visibility:**

| Role | SSN/PAN (PCI) | Name/Email (PII) | Amount/Date |
|------|:---:|:---:|:---:|
| Fraud Analyst | Visible | Visible | Visible |
| Marketing Analyst | NULL | Visible | Visible |
| Junior Analyst | NULL | NULL | Visible |

## Project Structure

```
pme/
├── src/
│   ├── kms_client.py      # AwsKmsClient — PyArrow ↔ AWS KMS bridge
│   ├── encryption.py      # Write pipeline: encrypt → PME Parquet
│   ├── decryption.py      # Read pipeline (TODO)
│   └── config.py          # Column→key mappings, KMS ARNs, S3 paths
├── tests/
│   ├── conftest.py        # InMemoryKmsClient fixture (no AWS needed)
│   ├── test_kms_client.py
│   └── test_encryption.py
├── pyproject.toml
└── requirements.txt
```

## Quick Start

```bash
# Install dependencies
cd pme
pip install -r requirements.txt

# Run unit tests (no AWS credentials needed)
python -m pytest tests/ -v -m "not integration"

# Verify PyArrow encryption module
python -c "from pyarrow.parquet.encryption import CryptoFactory; print('PME ready')"
```

## Implementation Plan

See [PME_IMPLEMENTATION_PLAN.md](PME_IMPLEMENTATION_PLAN.md) for the full phased plan, architecture details, encryption/decryption flows, and commit strategy.
