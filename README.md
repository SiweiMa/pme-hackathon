# PME — Parquet Modular Encryption Hackathon

Column-level encryption for Parquet files using PyArrow + AWS KMS. Same file, different visibility per IAM role — encrypted by default, decrypted by permission.

**Winter Hackathon 2026 · 3 engineers**

## How It Works

```
Write:  S3 event → Lambda (container) → PyArrow PME encrypt → S3 (per-column KMS-encrypted Parquet)
Read:   Athena SQL → Lambda Federated Connector → PyArrow PME decrypt + RBAC → Arrow IPC → Athena results
```

| Role | PCI (SSN) | PII (Name, Email) | Non-sensitive (xid, balance) |
|------|:---:|:---:|:---:|
| Fraud Analyst | Visible | Visible | Visible |
| Marketing Analyst | NULL | Visible | Visible |
| Junior Analyst | NULL | NULL | Visible |

## Progress

```
[===========================>---] 90%  (11/12)
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
| Lambda encrypt container image + S3 event trigger | :white_check_mark: |
| Glue catalog registration | :white_check_mark: |
| Athena Federated Connector (Lambda decrypt proxy + RBAC) | :white_check_mark: |
| Benchmarks | :x: |

> **Architecture change:** Athena Spark was dropped — PySpark's JVM Parquet reader does not support PME. A Python Lambda Federated Connector implements the Athena Federation wire protocol, decrypting PME data on-the-fly and returning Arrow IPC to Athena.

## Architecture

```
                          ┌──────────────────────┐
  CSV upload to S3  ───►  │  Encrypt Lambda      │  ──► S3 (PME Parquet)
                          │  (PyArrow + 3 KMS)   │       │
                          └──────────────────────┘       │
                                                          │
  Athena SQL query  ───►  ┌──────────────────────┐       │
                          │  Connector Lambda     │  ◄───┘
                          │  (decrypt + RBAC)     │
                          │  ┌────────────────┐   │
                          │  │ caller identity │   │
                          │  │ → denied keys   │   │
                          │  │ → null masking  │   │
                          │  └────────────────┘   │
                          └──────────────────────┘
                                    │
                          Arrow IPC (wire protocol)
                                    │
                                    ▼
                          Athena / QuickSight results
```

**Key components:**

| Component | Path | Description |
|-----------|------|-------------|
| PME library | `pme/src/` | Encryption, decryption, KMS client, config |
| Encrypt Lambda | `lambda/` | S3-triggered CSV → PME Parquet encryption |
| Federated Connector | `connector/` | Athena Federation protocol, decrypt + RBAC |
| Infrastructure | `infra/` | Terraform — KMS, IAM, S3, Lambda, ECR, Athena |

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

### Query via Athena

```sql
-- Use the federated connector catalog
SELECT * FROM "pwe-hackathon-pme-connector"."pwe-hackathon-pme-db"."customer_data" LIMIT 10;
```

Switch IAM roles to see different column visibility per the RBAC matrix above.

## Docs

See [PME_IMPLEMENTATION_PLAN.md](PME_IMPLEMENTATION_PLAN.md) for full architecture, encryption flows, and phased plan.
