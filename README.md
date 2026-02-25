# PME — Parquet Modular Encryption Hackathon

Column-level encryption for Parquet files using PyArrow + AWS KMS. Same file, different visibility per IAM role — encrypted by default, decrypted by permission.

**Winter Hackathon 2026 · 3 engineers**

## How It Works

```
Write:  DataFrame → PyArrow PME → S3 (per-column KMS-encrypted Parquet)
Read:   Athena Spark / Snowflake+Lambda → PyArrow decrypt → IAM role governs column access
```

| Role | PCI (SSN, PAN) | PII (Name, Email) | Non-sensitive |
|------|:---:|:---:|:---:|
| Fraud Analyst | Visible | Visible | Visible |
| Marketing Analyst | NULL | Visible | Visible |
| Junior Analyst | NULL | NULL | Visible |

## Progress

```
[========>-----------------------] 33%  (4/12)
```

| Phase | Status |
|-------|--------|
| Project skeleton + config | :white_check_mark: |
| KMS client (PyArrow ↔ AWS KMS) | :white_check_mark: |
| Encryption write pipeline | :white_check_mark: |
| Sample data (`Hackathon_customer_data.csv`) | :white_check_mark: |
| CI (GitHub Actions, Python 3.10–3.12) | :white_check_mark: |
| Decryption read pipeline + RBAC | :x: |
| AWS infra — KMS, IAM, S3 (Terraform) | :x: |
| Athena Spark + QuickSight | :x: |
| Lambda + API Gateway | :x: |
| Snowflake external functions | :x: |
| Benchmarks | :x: |

## Quick Start

```bash
cd pme && pip install -r requirements.txt
python -m pytest tests/ -v -m "not integration"
```

## Docs

See [PME_IMPLEMENTATION_PLAN.md](PME_IMPLEMENTATION_PLAN.md) for full architecture, encryption flows, and phased plan.
