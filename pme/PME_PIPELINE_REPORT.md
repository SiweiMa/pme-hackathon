# PME Pipeline Report: Write & Read with Column-Level Encryption

## Overview

This report shows the Parquet Modular Encryption (PME) pipeline running end-to-end on `Hackathon_customer_data.csv`. All output below is from an actual run of `pme/demo_read_pipeline.py`.

To reproduce:

```bash
cd pme && python -m pme.demo_read_pipeline
```

---

## 1. Load Sample Data

```
Loaded 100 rows, 6 columns
Columns: ['first_name', 'last_name', 'ssn', 'email', 'xid', 'balance']
Schema:
  first_name: string
  last_name: string
  ssn: string
  email: string
  xid: string
  balance: double

First 5 rows:
first_name last_name         ssn                    email      xid  balance
     James    Miller 999-01-1001 james.miller@example.com XID04821  4520.50
     Maria    Garcia 999-01-1002    m.garcia@testmail.org XID92103   120.75
    Robert     Smith 999-01-1003    rsmith88@provider.net XID55291  8943.20
     Linda   Johnson 999-01-1004      linda.j@company.com XID11034    25.00
   Michael     Brown 999-01-1005   mike.brown@webmail.com XID88273 15600.44
```

---

## 2. Encryption Config

Each column is assigned a sensitivity tier, and each tier maps to a separate KMS CMK:

```
Footer key alias: pme-footer-key
PCI key alias: pme-pci-key  →  columns: ['ssn']
PII key alias: pme-pii-key  →  columns: ['first_name', 'last_name', 'email']
Non-encrypted columns: ['xid', 'balance']
Algorithm: AES_GCM_V1
Plaintext footer: True
```

| Column | Tier | KMS Key (alias) | Encrypted? |
|--------|------|------------------|------------|
| `ssn` | PCI | `pme-pci-key` | Yes |
| `first_name` | PII | `pme-pii-key` | Yes |
| `last_name` | PII | `pme-pii-key` | Yes |
| `email` | PII | `pme-pii-key` | Yes |
| `xid` | Non-sensitive | — | No |
| `balance` | Non-sensitive | — | No |

---

## 3. Write Pipeline (Encryption)

**Entry point:** `write_encrypted_parquet(table, path, config, kms_client_factory)`

### Actual result

```
Writing encrypted Parquet...
    File: customer_data_encrypted.parquet
    Size: 8,774 bytes
    Magic bytes: b'PAR1' (plaintext footer)
```

### What happens internally

1. PyArrow generates a random 256-bit AES DEK per column group:
   - `DEK_pci` for `ssn`
   - `DEK_pii` for `first_name`, `last_name`, `email`
   - `DEK_footer` for the Parquet footer

2. Each DEK is wrapped (encrypted) by KMS via `wrap_key(DEK, alias)`:
   - `wrap_key(DEK_pci, "pme-pci-key")` → KMS returns encrypted blob
   - `wrap_key(DEK_pii, "pme-pii-key")` → KMS returns encrypted blob
   - `wrap_key(DEK_footer, "pme-footer-key")` → KMS returns encrypted blob

3. Column data is encrypted locally with the plaintext DEKs (AES-GCM):
   - `ssn` bytes + `DEK_pci` → encrypted bytes
   - `first_name`, `last_name`, `email` bytes + `DEK_pii` → encrypted bytes
   - `xid`, `balance` → stored as plaintext (not encrypted)

4. File is written. Wrapped DEKs are stored in the Parquet footer. Plaintext DEKs are discarded.

### File structure

```
┌──────────────────────────────────────────────┐
│ Magic bytes: PAR1 (plaintext footer)         │
├──────────────────────────────────────────────┤
│ Row Group 0                                  │
│   ├─ ssn          [AES-GCM encrypted]        │
│   ├─ first_name   [AES-GCM encrypted]        │
│   ├─ last_name    [AES-GCM encrypted]        │
│   ├─ email        [AES-GCM encrypted]        │
│   ├─ xid          [plaintext]                │
│   └─ balance      [plaintext]                │
├──────────────────────────────────────────────┤
│ Footer (signed, plaintext readable)          │
│   ├─ Schema: column names + types            │
│   ├─ wrapped_DEK_pci  (KMS ciphertext)       │
│   ├─ wrapped_DEK_pii  (KMS ciphertext)       │
│   └─ wrapped_DEK_foot (KMS ciphertext)       │
└──────────────────────────────────────────────┘
```

---

## 4. Schema Discovery (No Keys Required)

With `plaintext_footer=True`, anyone can read the schema without decryption keys:

```
Reading schema WITHOUT decryption keys (plaintext footer)...
    Schema columns: ['first_name', 'last_name', 'ssn', 'email', 'xid', 'balance']
    (No KMS keys needed — footer is plaintext)
```

But attempting to read the **data** without keys fails:

```
Attempting to read DATA without decryption keys...
    Correctly failed: OSError
```

---

## 5. Read Pipeline (Decryption)

**Entry point:** `read_encrypted_parquet(path, config, kms_client_factory)`

### Actual result — full access (fraud analyst role)

```
Reading with FULL ACCESS (fraud analyst role)...
    Recovered 100 rows, 6 columns
    Data matches original: True

    First 5 rows:
first_name last_name         ssn                    email      xid  balance
     James    Miller 999-01-1001 james.miller@example.com XID04821  4520.50
     Maria    Garcia 999-01-1002    m.garcia@testmail.org XID92103   120.75
    Robert     Smith 999-01-1003    rsmith88@provider.net XID55291  8943.20
     Linda   Johnson 999-01-1004      linda.j@company.com XID11034    25.00
   Michael     Brown 999-01-1005   mike.brown@webmail.com XID88273 15600.44
```

The decrypted data is **identical** to the original CSV input.

### What happens internally

1. PyArrow reads wrapped DEKs from the Parquet footer
2. Each wrapped DEK is sent to KMS via `unwrap_key(wrapped_DEK, alias)`:
   - `unwrap_key(wrapped_DEK_pci, "pme-pci-key")` → returns plaintext `DEK_pci`
   - `unwrap_key(wrapped_DEK_pii, "pme-pii-key")` → returns plaintext `DEK_pii`
   - `unwrap_key(wrapped_DEK_footer, "pme-footer-key")` → returns plaintext `DEK_footer`
3. Column data is decrypted locally with the plaintext DEKs (AES-GCM)
4. Full PyArrow Table is returned

### Column selection

The `columns=` parameter filters the returned columns, but **all keys must still be available** (PyArrow decrypts all column metadata regardless):

```
Reading with columns=['xid', 'balance'] (full access)...
    Recovered 100 rows, 2 columns
    Columns: ['xid', 'balance']

    First 5 rows:
     xid  balance
XID04821  4520.50
XID92103   120.75
XID55291  8943.20
XID11034    25.00
XID88273 15600.44
```

---

## 6. File Metadata

```
File metadata (via get_file_metadata)...
    num_rows: 100
    num_row_groups: 1
    num_columns: 6
    created_by: parquet-cpp-arrow version 23.0.1
```

---

## 7. RBAC Access Control

Access is controlled at the **KMS key level**. Different IAM roles are granted `kms:Decrypt` on different CMKs. Because PyArrow's C++ reader processes metadata for **ALL** columns during any read, reads are **all-or-nothing**: if any column key is denied, the entire read fails.

### Role Definitions

| Role | PCI Key (`pme-pci-key`) | PII Key (`pme-pii-key`) | Footer Key | Result |
|------|-------------------------|-------------------------|------------|--------|
| **Fraud Analyst** | Decrypt | Decrypt | Decrypt | Reads all columns |
| **Marketing Analyst** | **Denied** | Decrypt | Decrypt | Read fails |
| **Junior Analyst** | **Denied** | **Denied** | Decrypt | Read fails |

### Actual result — Marketing Analyst (PCI denied)

```
RBAC: Marketing analyst (PCI key DENIED)...
    Correctly denied: Access denied: kms:Decrypt not allowed for key 'pme-pci-key'
```

The read fails entirely. No partial data is returned — even the non-sensitive `xid` and `balance` columns are inaccessible.

### Actual result — Junior Analyst (PCI + PII denied)

```
RBAC: Junior analyst (PCI + PII keys DENIED)...
    Correctly denied: Access denied: kms:Decrypt not allowed for key 'pme-pii-key'
```

Denied on the first encrypted column group encountered. No data is returned.

### How to grant access in AWS

Each role's IAM policy controls which KMS keys it can use:

```json
{
    "Effect": "Allow",
    "Action": ["kms:Decrypt"],
    "Resource": [
        "arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-footer-key",
        "arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-pci-key",
        "arn:aws:kms:us-east-2:651767347247:alias/pwe-hackathon-pii-key"
    ]
}
```

Remove a resource from the list to deny that tier. Under the all-or-nothing model, all three keys must be granted for reads to succeed.

---

## 8. Encrypted Footer Mode

When `plaintext_footer=False`, the footer itself is encrypted — schema discovery without keys is blocked:

```
Encrypted footer mode (plaintext_footer=False)...
    Magic bytes: b'PARE' (encrypted footer)
    File size: 8,589 bytes
    Schema without keys: NOT readable (footer is encrypted)
    Read with keys: 100 rows, data matches: True
```

| Footer Mode | Magic Bytes | Schema without keys | File size |
|-------------|-------------|---------------------|-----------|
| `plaintext_footer=True` | `PAR1` | Readable | 8,774 bytes |
| `plaintext_footer=False` | `PARE` | Not readable | 8,589 bytes |

---

## 9. Security Summary

| Property | Status |
|----------|--------|
| Column data encrypted at rest (AES-GCM) | Yes |
| DEKs encrypted by AWS KMS (envelope encryption) | Yes |
| Plaintext DEKs never written to disk | Yes |
| Different KMS keys per sensitivity tier | Yes |
| Access controlled via IAM `kms:Decrypt` grants | Yes |
| Partial column reads bypass encryption | No — all-or-nothing |
| Schema discoverable without keys (plaintext footer) | Configurable |

## 10. File Inventory

| File | Purpose |
|------|---------|
| `pme/src/config.py` | Configuration: KMS keys, column groups, algorithm settings |
| `pme/src/encryption.py` | Write pipeline: `write_encrypted_parquet()`, `write_encrypted_to_s3()` |
| `pme/src/decryption.py` | Read pipeline: `read_encrypted_parquet()`, `read_encrypted_from_s3()` |
| `pme/src/kms_client.py` | AWS KMS integration: `AwsKmsClient`, `AwsKmsClientFactory` |
| `pme/demo_read_pipeline.py` | Demo script that produced this report's output |
| `pme/tests/conftest.py` | Test fixtures: `InMemoryKmsClient`, RBAC role factories |
| `pme/tests/test_encryption.py` | Write pipeline unit tests |
| `pme/tests/test_decryption.py` | Read pipeline + RBAC unit tests |
| `pme/tests/test_integration.py` | Integration tests with real AWS KMS + S3 |
