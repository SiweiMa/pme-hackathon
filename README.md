# PME — Parquet Modular Encryption Hackathon

Column-level encryption for Parquet files using PyArrow + AWS KMS. Same file, different visibility per IAM role — encrypted by default, decrypted by permission.

**Winter Hackathon 2026 · 3 engineers**

## How It Works

```
Write:  S3 event → Lambda (container) → PyArrow PME encrypt → S3 (per-column KMS-encrypted Parquet)
Read:   Athena SQL → Lambda Federated Connector → PyArrow PME decrypt + RBAC → Arrow IPC → Athena results
Join:   Athena cross-catalog → PME customer_data (Lambda) JOIN Iceberg auth_data (Glue/S3) ON xid
```

| Role | PCI (SSN) | PII (Name, Email) | Non-sensitive (xid, balance) |
|------|:---:|:---:|:---:|
| Fraud Analyst | Visible | Visible | Visible |
| Marketing Analyst | NULL | Visible | Visible |
| Junior Analyst | NULL | NULL | Visible |

## Progress

```
[==============================>] 95%  (13/14)
```

| Phase | Status |
|-------|--------|
| Project skeleton + config | :white_check_mark: |
| KMS client (PyArrow ↔ AWS KMS) | :white_check_mark: |
| Encryption write pipeline | :white_check_mark: |
| Sample data (`Hackathon_customer_data.csv` + `Hackathon_auth_data.csv`) | :white_check_mark: |
| CI/CD (GitHub Actions — tests + Terraform) | :white_check_mark: |
| AWS infra — KMS keys, IAM RBAC roles, S3 (Terraform) | :white_check_mark: |
| Integration tests + demo encrypt script | :white_check_mark: |
| Decryption read pipeline + RBAC | :white_check_mark: |
| Lambda encrypt container image + S3 event trigger | :white_check_mark: |
| Glue catalog registration | :white_check_mark: |
| Athena Federated Connector (Lambda decrypt proxy + RBAC) | :white_check_mark: |
| Snowflake Iceberg table + S3 storage | :white_check_mark: |
| Athena cross-catalog join (PME + Iceberg) | :white_check_mark: |
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
                          Athena (pme-hackathon-pme-connector)
                                    │
                                    │  JOIN ON xid
                                    │
  Snowflake Iceberg ───►  S3 (Parquet + metadata)
                                    │
                          Glue (pme-hackathon-iceberg-db)
                                    │
                                    ▼
                          Athena (AwsDataCatalog) ──► cross-catalog results
```

**Key components:**

| Component | Path | Description |
|-----------|------|-------------|
| PME library | `pme/src/` | Encryption, decryption, KMS client, config |
| Encrypt Lambda | `lambda/` | S3-triggered CSV → PME Parquet encryption |
| Federated Connector | `connector/` | Athena Federation protocol, decrypt + RBAC |
| Snowflake Iceberg | `snowflake/` | Iceberg table DDL, Glue registration script |
| Infrastructure | `infra/` | Terraform — KMS, IAM, S3, Lambda, ECR, Athena, Iceberg |

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
-- PME-encrypted customer data (column visibility depends on caller's IAM role)
SELECT * FROM "pme-hackathon-pme-connector"."pme-hackathon-pme-db"."customer_data" LIMIT 10;

-- Iceberg auth transactions (plaintext, accessible to all analyst roles)
SELECT * FROM "AwsDataCatalog"."pme-hackathon-iceberg-db"."auth_data" LIMIT 10;

-- Cross-catalog join: PME customer_data + Iceberg auth_data
SELECT c.first_name, c.last_name, c.email, c.xid, c.balance,
       a.auth_id, a.auth_ts, a.merchant, a.amount
FROM "pme-hackathon-pme-connector"."pme-hackathon-pme-db"."customer_data" c
JOIN "AwsDataCatalog"."pme-hackathon-iceberg-db"."auth_data" a
  ON c.xid = a.xid
ORDER BY a.auth_ts DESC;
```

Switch IAM roles to see different column visibility per the RBAC matrix above. The Iceberg auth_data side is always fully visible; RBAC only applies to PME-encrypted customer_data columns.

## Docs

| Document | Description |
|----------|-------------|
| [PME_IMPLEMENTATION_PLAN.md](docs/PME_IMPLEMENTATION_PLAN.md) | Full architecture, encryption flows, and phased plan |
| [SNOWFLAKE_ICEBERG_SETUP.md](docs/SNOWFLAKE_ICEBERG_SETUP.md) | Snowflake Iceberg table + Athena cross-catalog join setup |
| [ATHENA_DECRYPTION_GUIDE.md](docs/ATHENA_DECRYPTION_GUIDE.md) | Athena Federated Connector decryption guide |
| [PME_FAQ.md](docs/PME_FAQ.md) | FAQ for PME and Athena federation |
| [PME_PIPELINE_REPORT.md](docs/PME_PIPELINE_REPORT.md) | PME pipeline test report |
| [SPARK_PME_CHALLENGE.md](docs/SPARK_PME_CHALLENGE.md) | Why Athena Spark was dropped |
