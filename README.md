# PME — Parquet Modular Encryption Hackathon

Column-level encryption for Parquet files using PyArrow + AWS KMS. Same file, different visibility per IAM role — encrypted by default, decrypted by permission.

**Winter Hackathon 2026 · 3 engineers**

## How It Works

```
Write:  Lambda (container image) → PyArrow PME → S3 (per-column KMS-encrypted Parquet)
Read:   Lambda (container image) → PyArrow decrypt → IAM execution role governs column access
```

| Role | PCI (SSN, PAN) | PII (Name, Email) | Non-sensitive |
|------|:---:|:---:|:---:|
| Fraud Analyst | Visible | Visible | Visible |
| Marketing Analyst | NULL | Visible | Visible |
| Junior Analyst | NULL | NULL | Visible |

## Progress

```
[==================>-------------] 62%  (8/13)
```

| Phase | Status |
|-------|--------|
| Project skeleton + config | :white_check_mark: |
| KMS client (PyArrow ↔ AWS KMS) | :white_check_mark: |
| Encryption write pipeline | :white_check_mark: |
| Sample data (`Hackathon_customer_data.csv`) | :white_check_mark: |
| CI/CD (GitHub Actions — tests + Terraform) | :white_check_mark: |
| AWS infra — KMS keys, IAM RBAC roles, S3 (Terraform) | :white_check_mark: |
| Integration tests + demo encrypt script | :white_check_mark: |
| Decryption read pipeline + RBAC | :white_check_mark: |
| Lambda container image (encrypt + decrypt) | :x: |
| Lambda RBAC demo (3 roles, same file) | :x: |
| API Gateway + Snowflake external functions | :x: |
| Benchmarks | :x: |

> **Architecture change:** Athena Spark was dropped — PySpark's JVM Parquet reader does not support PME. Lambda container image (PyArrow native C++) is now the compute for both encrypt and decrypt.

## Quick Start

```bash
cd pme && pip install -r requirements.txt

# unit tests (no AWS creds needed)
python -m pytest tests/ -v -m "not integration"

# integration tests (requires AWS creds + KMS keys)
python -m pytest tests/ -v -m integration

# encrypt sample data to S3
python demo_encrypt.py

# inspect an encrypted Parquet file
python inspect_encrypted.py <path-to-file.parquet>
```

## Docs

See [PME_IMPLEMENTATION_PLAN.md](PME_IMPLEMENTATION_PLAN.md) for full architecture, encryption flows, and phased plan.
