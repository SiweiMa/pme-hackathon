# PME Pipeline Report: Write & Read with Column-Level Encryption

## Overview

This report explains how the Parquet Modular Encryption (PME) pipeline works end-to-end using concrete examples from `Hackathon_customer_data.csv`.

---

## 1. Sample Data

The CSV has 100 rows and 6 columns:

```
first_name,last_name,ssn,email,xid,balance
James,Miller,999-01-1001,james.miller@example.com,XID04821,4520.5
Maria,Garcia,999-01-1002,m.garcia@testmail.org,XID92103,120.75
Robert,Smith,999-01-1003,rsmith88@provider.net,XID55291,8943.2
...
```

## 2. Column Classification

Each column is assigned a sensitivity tier, and each tier gets its own AWS KMS CMK:

| Column | Tier | KMS Key (alias) | Encrypted? |
|--------|------|------------------|------------|
| `ssn` | PCI | `pwe-hackathon-pci-key` | Yes |
| `first_name` | PII | `pwe-hackathon-pii-key` | Yes |
| `last_name` | PII | `pwe-hackathon-pii-key` | Yes |
| `email` | PII | `pwe-hackathon-pii-key` | Yes |
| `xid` | Non-sensitive | — | No |
| `balance` | Non-sensitive | — | No |

A third key, `pwe-hackathon-footer-key`, encrypts/signs the Parquet footer (file metadata).

---

## 3. Write Pipeline (Encryption)

**Entry point:** `write_encrypted_parquet(table, path, config, kms_client_factory)`

### Step-by-step with example data

```
Input: PyArrow Table loaded from Hackathon_customer_data.csv (100 rows)

    first_name  last_name  ssn           email                      xid       balance
    James       Miller     999-01-1001   james.miller@example.com   XID04821  4520.5
    Maria       Garcia     999-01-1002   m.garcia@testmail.org      XID92103  120.75
    ...
```

**Step 1 — PyArrow generates random DEKs (Data Encryption Keys)**

For each column group, PyArrow generates a random 256-bit AES key:

```
DEK_pci  = <random 32 bytes>    → encrypts: ssn
DEK_pii  = <random 32 bytes>    → encrypts: first_name, last_name, email
DEK_foot = <random 32 bytes>    → encrypts: Parquet footer
```

**Step 2 — DEKs are wrapped (encrypted) by KMS**

PyArrow calls `KmsClient.wrap_key(DEK, key_alias)` for each DEK:

```
wrap_key(DEK_pci,  "pwe-hackathon-pci-key")
    → PyArrow sends DEK_pci to AWS KMS
    → KMS encrypts DEK_pci using CMK alias/pwe-hackathon-pci-key
    → Returns wrapped_DEK_pci (ciphertext blob)

wrap_key(DEK_pii,  "pwe-hackathon-pii-key")
    → Returns wrapped_DEK_pii

wrap_key(DEK_foot, "pwe-hackathon-footer-key")
    → Returns wrapped_DEK_foot
```

**Step 3 — Column data encrypted locally with plaintext DEKs**

PyArrow encrypts each column's data in memory using AES-GCM:

```
ssn column bytes          + DEK_pci  → [encrypted ssn bytes]
first_name column bytes   + DEK_pii  → [encrypted first_name bytes]
last_name column bytes    + DEK_pii  → [encrypted last_name bytes]
email column bytes        + DEK_pii  → [encrypted email bytes]

xid column bytes          → [plaintext — not encrypted]
balance column bytes      → [plaintext — not encrypted]
```

**Step 4 — Write Parquet file**

The file is written with this structure:

```
┌──────────────────────────────────────────────┐
│ Magic bytes: PAR1 (plaintext footer)         │
├──────────────────────────────────────────────┤
│ Row Group 0                                  │
│   ├─ ssn          [AES-GCM encrypted bytes]  │
│   ├─ first_name   [AES-GCM encrypted bytes]  │
│   ├─ last_name    [AES-GCM encrypted bytes]  │
│   ├─ email        [AES-GCM encrypted bytes]  │
│   ├─ xid          [plaintext bytes]          │
│   └─ balance      [plaintext bytes]          │
├──────────────────────────────────────────────┤
│ Footer (signed, plaintext readable)          │
│   ├─ Schema: column names + types            │
│   ├─ wrapped_DEK_pci  (KMS ciphertext)       │
│   ├─ wrapped_DEK_pii  (KMS ciphertext)       │
│   └─ wrapped_DEK_foot (KMS ciphertext)       │
└──────────────────────────────────────────────┘
```

**Key point:** The plaintext DEKs are never written to disk. Only the KMS-wrapped (encrypted) DEKs are stored in the Parquet footer.

### What the file looks like without keys

If someone opens the file without decryption keys:

- `xid` and `balance` are **readable** (not encrypted)
- `ssn`, `first_name`, `last_name`, `email` are **garbled** (encrypted bytes)
- Schema (column names/types) is **readable** when `plaintext_footer=True`
- Schema is also encrypted when `plaintext_footer=False`

---

## 4. Read Pipeline (Decryption)

**Entry point:** `read_encrypted_parquet(path, config, kms_client_factory)`

### Step-by-step

```
Input: encrypted Parquet file from Step 3
```

**Step 1 — PyArrow reads wrapped DEKs from the Parquet footer**

```
Footer contains:
    wrapped_DEK_pci  → tagged with alias "pwe-hackathon-pci-key"
    wrapped_DEK_pii  → tagged with alias "pwe-hackathon-pii-key"
    wrapped_DEK_foot → tagged with alias "pwe-hackathon-footer-key"
```

**Step 2 — DEKs are unwrapped (decrypted) by KMS**

PyArrow calls `KmsClient.unwrap_key(wrapped_DEK, key_alias)` for each:

```
unwrap_key(wrapped_DEK_pci, "pwe-hackathon-pci-key")
    → PyArrow sends wrapped_DEK_pci to AWS KMS
    → KMS decrypts it using CMK alias/pwe-hackathon-pci-key
    → Returns plaintext DEK_pci

unwrap_key(wrapped_DEK_pii, "pwe-hackathon-pii-key")
    → Returns plaintext DEK_pii

unwrap_key(wrapped_DEK_foot, "pwe-hackathon-footer-key")
    → Returns plaintext DEK_foot
```

**Step 3 — Column data decrypted locally**

```
[encrypted ssn bytes]        + DEK_pci  → "999-01-1001", "999-01-1002", ...
[encrypted first_name bytes] + DEK_pii  → "James", "Maria", ...
[encrypted last_name bytes]  + DEK_pii  → "Miller", "Garcia", ...
[encrypted email bytes]      + DEK_pii  → "james.miller@example.com", ...

[plaintext xid bytes]        → "XID04821", "XID92103", ...  (no decryption needed)
[plaintext balance bytes]    → 4520.5, 120.75, ...           (no decryption needed)
```

**Step 4 — Return PyArrow Table**

```
Output: identical to the original input

    first_name  last_name  ssn           email                      xid       balance
    James       Miller     999-01-1001   james.miller@example.com   XID04821  4520.5
    Maria       Garcia     999-01-1002   m.garcia@testmail.org      XID92103  120.75
    ...
```

---

## 5. RBAC Access Control

Access is controlled at the **KMS key level**. Different IAM roles are granted `kms:Decrypt` on different CMKs.

### Important: All-or-Nothing Reads

PyArrow's C++ reader processes metadata for **ALL** columns during any read — even if you request `columns=["xid", "balance"]`. This triggers `unwrap_key` for every encrypted column group. If any key is denied, the entire read fails.

### Role Definitions

| Role | PCI Key | PII Key | Footer Key | Can Read? |
|------|---------|---------|------------|-----------|
| **Fraud Analyst** | Decrypt | Decrypt | Decrypt | All columns |
| **Marketing Analyst** | Denied | Decrypt | Decrypt | Fails (PCI denied) |
| **Junior Analyst** | Denied | Denied | Decrypt | Fails (PCI+PII denied) |

### Example: Fraud Analyst (full access)

```python
factory = AwsKmsClientFactory(region="us-east-2", role_arn="arn:...fraud-analyst-role")
table = read_encrypted_parquet("data.parquet", config, factory)

# Result: all 100 rows, all 6 columns — succeeds
#   ssn           first_name  last_name  email                      xid       balance
#   999-01-1001   James       Miller     james.miller@example.com   XID04821  4520.5
#   ...
```

### Example: Marketing Analyst (PCI denied)

```python
factory = AwsKmsClientFactory(region="us-east-2", role_arn="arn:...marketing-analyst-role")
table = read_encrypted_parquet("data.parquet", config, factory)

# Result: PermissionError!
# "Access denied: kms:Decrypt not allowed for key 'pwe-hackathon-pci-key'"
#
# The read fails entirely — no partial data is returned.
# Even though marketing only wanted email addresses, PyArrow tried to
# unwrap ALL column keys including PCI, which was denied.
```

### Example: Junior Analyst (PCI + PII denied)

```python
factory = AwsKmsClientFactory(region="us-east-2", role_arn="arn:...junior-analyst-role")
table = read_encrypted_parquet("data.parquet", config, factory)

# Result: PermissionError!
# Denied on first encrypted column group encountered (PCI or PII).
# Cannot read any data, even the non-sensitive xid and balance columns.
```

### How to grant access in AWS

Each role's IAM policy controls which KMS keys it can use:

```json
{
    "Effect": "Allow",
    "Action": ["kms:Decrypt"],
    "Resource": [
        "arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-footer-key",
        "arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-pii-key"
    ]
}
```

This policy grants PII + footer access but **not** PCI — matching the Marketing Analyst role. Under the all-or-nothing model, this role's reads will fail because PCI access is still required.

---

## 6. Code Examples

### Write (encrypt) the CSV

```python
from pme.src.config import PmeConfig, KmsKeyConfig, ColumnGroupConfig
from pme.src.encryption import write_encrypted_parquet
from pme.src.kms_client import AwsKmsClientFactory

config = PmeConfig(
    footer_key=KmsKeyConfig(
        key_arn="arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-footer-key",
        alias="pwe-hackathon-footer-key",
    ),
    column_groups=[
        ColumnGroupConfig(
            name="pci",
            kms_key=KmsKeyConfig(
                key_arn="arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-pci-key",
                alias="pwe-hackathon-pci-key",
            ),
            columns=["ssn"],
        ),
        ColumnGroupConfig(
            name="pii",
            kms_key=KmsKeyConfig(
                key_arn="arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-pii-key",
                alias="pwe-hackathon-pii-key",
            ),
            columns=["first_name", "last_name", "email"],
        ),
    ],
    region="us-east-2",
)

factory = AwsKmsClientFactory(region="us-east-2", alias_to_arn=config.alias_to_arn)

# Load CSV into PyArrow Table
import csv, pyarrow as pa
rows = []
with open("Hackathon_customer_data.csv") as f:
    for row in csv.DictReader(f):
        row["balance"] = float(row["balance"])
        rows.append(row)
table = pa.Table.from_pylist(rows)

# Write encrypted
write_encrypted_parquet(table, "customer_data.parquet", config, factory)
```

### Read (decrypt) the file

```python
from pme.src.decryption import read_encrypted_parquet

table = read_encrypted_parquet("customer_data.parquet", config, factory)

print(table.to_pandas().head())
#   first_name last_name          ssn                     email      xid  balance
# 0      James    Miller  999-01-1001  james.miller@example.com  XID04821  4520.50
# 1      Maria    Garcia  999-01-1002     m.garcia@testmail.org  XID92103   120.75
# 2     Robert     Smith  999-01-1003      rsmith88@provider.net  XID55291  8943.20
```

### Read from S3

```python
from pme.src.encryption import write_encrypted_to_s3
from pme.src.decryption import read_encrypted_from_s3

# Write to S3
write_encrypted_to_s3(table, config, factory, filename="customers.parquet")

# Read from S3
recovered = read_encrypted_from_s3(config, factory, filename="customers.parquet")
```

### Inspect metadata without decryption keys

```python
import pyarrow.parquet as pq

# Works with plaintext_footer=True — no KMS keys needed
schema = pq.read_schema("customer_data.parquet")
print(schema)
# first_name: string
# last_name: string
# ssn: string
# email: string
# xid: string
# balance: double
```

---

## 7. Security Summary

| Property | Status |
|----------|--------|
| Column data encrypted at rest (AES-GCM) | Yes |
| DEKs encrypted by AWS KMS (envelope encryption) | Yes |
| Plaintext DEKs never written to disk | Yes |
| Different KMS keys per sensitivity tier | Yes |
| Access controlled via IAM `kms:Decrypt` grants | Yes |
| Partial column reads bypass encryption | No — all-or-nothing |
| Schema discoverable without keys (plaintext footer) | Configurable |

## 8. File Inventory

| File | Purpose |
|------|---------|
| `pme/src/config.py` | Configuration: KMS keys, column groups, algorithm settings |
| `pme/src/encryption.py` | Write pipeline: `write_encrypted_parquet()`, `write_encrypted_to_s3()` |
| `pme/src/decryption.py` | Read pipeline: `read_encrypted_parquet()`, `read_encrypted_from_s3()` |
| `pme/src/kms_client.py` | AWS KMS integration: `AwsKmsClient`, `AwsKmsClientFactory` |
| `pme/tests/conftest.py` | Test fixtures: `InMemoryKmsClient`, RBAC role factories |
| `pme/tests/test_encryption.py` | Write pipeline unit tests |
| `pme/tests/test_decryption.py` | Read pipeline + RBAC unit tests |
| `pme/tests/test_integration.py` | Integration tests with real AWS KMS + S3 |
