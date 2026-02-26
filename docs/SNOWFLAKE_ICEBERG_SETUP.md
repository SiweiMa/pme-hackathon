# Snowflake Iceberg Table + Athena Cross-Catalog Join

How auth transaction data gets loaded into a Snowflake-managed Iceberg table on S3, registered in Glue, and joined with PME-encrypted customer data via Athena's Lambda federated connector.

## Architecture

```
CSV (auth data)
  │
  ▼
S3 bucket (pme-hackathon-iceberg-651767347247)
  │
  ├──► Snowflake (COPY INTO Iceberg table)
  │         │
  │         ▼
  │    Iceberg metadata + Parquet on S3
  │         │
  │         ▼
  ├──► Glue Catalog (pme-hackathon-iceberg-db.auth_data)
  │         │
  │         ▼
  │    Athena (AwsDataCatalog) ◄── reads Iceberg natively
  │                                        │
  │                                        │  JOIN ON xid
  │                                        │
  └──────────────────────── Athena (pme-hackathon-pme-connector)
                                    │
                                    ▼
                            Lambda decrypts PME ──► customer_data
```

## Data

- **Source CSV**: `data/Hackathon_auth_data.csv` (532 rows)
- **Columns**: `auth_id`, `xid`, `auth_ts`, `merchant`, `amount`
- **Join key**: `xid` (matches `customer_data.xid`)

## Setup Steps

### Step 1: Terraform — AWS Resources

Created `infra/iceberg.tf` with:

| Resource | Name |
|----------|------|
| S3 bucket | `pme-hackathon-iceberg-651767347247` |
| IAM role | `pme-hackathon-snowflake-iceberg` |
| Glue database | `pme-hackathon-iceberg-db` |

Modified `infra/iam.tf` to add Iceberg S3 read policies to all analyst roles (fraud, marketing, junior).

Modified `infra/outputs.tf` to add `iceberg_bucket_name`, `iceberg_bucket_arn`, `sf_iceberg_role_arn`.

```bash
cd infra/
terraform apply -var="aws_account_id=651767347247"
```

### Step 2: Upload CSV to S3

```bash
aws s3 cp data/Hackathon_auth_data.csv \
  s3://pme-hackathon-iceberg-651767347247/raw/auth_data/Hackathon_auth_data.csv
```

### Step 3: Snowflake — Create Database, Warehouse, Storage Integration

Run in Snowsight (or snowsql) as ACCOUNTADMIN:

```sql
CREATE DATABASE IF NOT EXISTS PME_HACKATHON;
USE DATABASE PME_HACKATHON;
CREATE SCHEMA IF NOT EXISTS PME_HACKATHON.PUBLIC;
USE SCHEMA PME_HACKATHON.PUBLIC;

CREATE WAREHOUSE IF NOT EXISTS PME_HACKATHON_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
USE WAREHOUSE PME_HACKATHON_WH;

CREATE OR REPLACE STORAGE INTEGRATION pme_hackathon_iceberg_s3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::651767347247:role/pme-hackathon-snowflake-iceberg'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://pme-hackathon-iceberg-651767347247/');
```

### Step 4: IAM Trust Bootstrap (Storage Integration)

Get the Snowflake principal info:

```sql
DESCRIBE STORAGE INTEGRATION pme_hackathon_iceberg_s3;
```

Note `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` from the output.

### Step 5: Snowflake — Create External Volume

```sql
CREATE OR REPLACE EXTERNAL VOLUME pme_hackathon_iceberg_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'pme-hackathon-iceberg-s3'
      STORAGE_BASE_URL = 's3://pme-hackathon-iceberg-651767347247/iceberg/'
      STORAGE_PROVIDER = 'S3'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::651767347247:role/pme-hackathon-snowflake-iceberg'
    )
  );

DESCRIBE EXTERNAL VOLUME pme_hackathon_iceberg_vol;
```

Note `STORAGE_AWS_EXTERNAL_ID` from the external volume output — it is **different** from the storage integration's external ID.

### Step 6: IAM Trust Bootstrap (Both External IDs)

Update the IAM trust policy with **both** external IDs (storage integration + external volume):

```bash
aws iam update-assume-role-policy \
  --role-name pme-hackathon-snowflake-iceberg \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"AWS": "<STORAGE_AWS_IAM_USER_ARN>"},
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": [
            "<STORAGE_INTEGRATION_EXTERNAL_ID>",
            "<EXTERNAL_VOLUME_EXTERNAL_ID>"
          ]
        }
      }
    }]
  }'
```

Wait ~60 seconds for IAM propagation.

### Step 7: Snowflake — Create Iceberg Table and Load Data

```sql
CREATE OR REPLACE ICEBERG TABLE PME_HACKATHON.PUBLIC.AUTH_DATA (
  auth_id   VARCHAR,
  xid       VARCHAR,
  auth_ts   TIMESTAMP_NTZ,
  merchant  VARCHAR,
  amount    FLOAT
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'pme_hackathon_iceberg_vol'
  BASE_LOCATION = 'auth_data/';

CREATE OR REPLACE STAGE pme_hackathon_auth_stage
  STORAGE_INTEGRATION = pme_hackathon_iceberg_s3
  URL = 's3://pme-hackathon-iceberg-651767347247/raw/auth_data/'
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

LIST @pme_hackathon_auth_stage;

COPY INTO PME_HACKATHON.PUBLIC.AUTH_DATA (auth_id, xid, auth_ts, merchant, amount)
  FROM @pme_hackathon_auth_stage
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
  ON_ERROR = 'ABORT_STATEMENT';

-- Verify
SELECT COUNT(*) FROM PME_HACKATHON.PUBLIC.AUTH_DATA;  -- Expected: 532
```

### Step 8: Register Iceberg Metadata in Glue

Snowflake writes Iceberg metadata to S3 with a random suffix on the base location. Discover the actual path:

```bash
aws s3 ls s3://pme-hackathon-iceberg-651767347247/iceberg/ --recursive \
  | grep metadata.json | sort | tail -1
```

Then create the Glue table pointing to the latest metadata:

```bash
aws glue create-table \
  --database-name pme-hackathon-iceberg-db \
  --table-input '{
    "Name": "auth_data",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {
      "table_type": "ICEBERG",
      "metadata_location": "s3://pme-hackathon-iceberg-651767347247/iceberg/<actual_path>/metadata/<latest>.metadata.json"
    },
    "StorageDescriptor": {
      "Location": "s3://pme-hackathon-iceberg-651767347247/iceberg/<actual_path>/",
      "Columns": [
        {"Name": "auth_id", "Type": "string"},
        {"Name": "xid", "Type": "string"},
        {"Name": "auth_ts", "Type": "timestamp"},
        {"Name": "merchant", "Type": "string"},
        {"Name": "amount", "Type": "double"}
      ]
    }
  }'
```

A helper script `snowflake/register_iceberg_glue.sh` automates this (update the prefix if the path differs).

### Step 9: Test in Athena

Use workgroup `pme-hackathon-pme-federated`.

**Test Iceberg table alone:**

```sql
SELECT * FROM "AwsDataCatalog"."pme-hackathon-iceberg-db"."auth_data" LIMIT 5;
```

**Cross-catalog join — PME-encrypted customer_data + Iceberg auth_data:**

```sql
SELECT
  c.first_name, c.last_name, c.email, c.xid, c.balance,
  a.auth_id, a.auth_ts, a.merchant, a.amount
FROM "pme-hackathon-pme-connector"."pme-hackathon-pme-db"."customer_data" c
JOIN "AwsDataCatalog"."pme-hackathon-iceberg-db"."auth_data" a
  ON c.xid = a.xid
ORDER BY a.auth_ts DESC;
```

**Aggregation — total spend per customer:**

```sql
SELECT
  c.first_name, c.last_name, c.email, c.xid, c.balance,
  COUNT(a.auth_id) AS auth_count,
  SUM(a.amount) AS total_auth_amount
FROM "pme-hackathon-pme-connector"."pme-hackathon-pme-db"."customer_data" c
JOIN "AwsDataCatalog"."pme-hackathon-iceberg-db"."auth_data" a
  ON c.xid = a.xid
GROUP BY c.first_name, c.last_name, c.email, c.xid, c.balance
ORDER BY total_auth_amount DESC;
```

## RBAC Behavior

The Lambda federated connector applies null-masking based on the caller's IAM role. The Iceberg auth_data side is plaintext (no encryption). So:

| Role | customer_data columns visible | auth_data |
|------|-------------------------------|-----------|
| Fraud Analyst | all (first_name, last_name, ssn, email, xid, balance) | all |
| Marketing Analyst | first_name, last_name, email, xid, balance (ssn = NULL) | all |
| Junior Analyst | xid, balance (PII + PCI = NULL) | all |

## Key Gotcha: Two External IDs

The storage integration and external volume generate **different** `STORAGE_AWS_EXTERNAL_ID` values. The IAM trust policy must include **both** in the `sts:ExternalId` condition, otherwise the external volume will fail with `Error assuming AWS_ROLE`.

## Files

| File | Purpose |
|------|---------|
| `infra/iceberg.tf` | S3 bucket, IAM role, Glue database |
| `infra/iam.tf` | Iceberg S3 read policies for analyst roles |
| `infra/outputs.tf` | Iceberg bucket/role ARN outputs |
| `snowflake/iceberg_setup.sql` | Complete Snowflake DDL |
| `snowflake/register_iceberg_glue.sh` | Glue metadata registration script |
