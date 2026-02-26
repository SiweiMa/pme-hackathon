# Spark + Parquet Modular Encryption: The Missing KMS Client

## The Short Version

Parquet Modular Encryption (PME) works across languages — a file encrypted by
PyArrow **can** be decrypted by Spark. The Parquet spec guarantees this.

The problem is not compatibility. The problem is that **Spark has no built-in
AWS KMS client**, so it cannot talk to AWS KMS to get the keys it needs to
decrypt (or encrypt) columns.

---

## Background

PME uses **envelope encryption**:

```
┌─────────────┐     ┌───────────┐     ┌──────────────┐
│ Column data  │────▶│ Encrypt   │────▶│ Encrypted    │
│ (plaintext)  │     │ with DEK  │     │ column data  │
└─────────────┘     └───────────┘     └──────────────┘
                          │
                    Random DEK
                          │
                    ┌─────▼─────┐     ┌──────────────┐
                    │ Wrap DEK  │────▶│ Wrapped DEK  │
                    │ with KMS  │     │ (in footer)  │
                    └───────────┘     └──────────────┘
```

1. A random **Data Encryption Key (DEK)** is generated per column group
2. Column data is encrypted locally with the DEK (AES-256-GCM)
3. The DEK itself is encrypted ("wrapped") by a **KMS master key**
4. The wrapped DEK is stored in the Parquet file footer

To decrypt, the process reverses: the reader asks KMS to unwrap the DEK, then
uses the plaintext DEK to decrypt the column data locally.

**The critical piece:** something must talk to KMS to wrap/unwrap the DEK. That
"something" is the **KMS client**.

---

## The Two Parquet Libraries

| | PyArrow (Python/C++) | parquet-mr (Java) |
|---|---|---|
| Used by | Pandas, Polars, DuckDB, our Lambda | **Spark**, Hive, Flink, Trino, Athena |
| PME support | Yes | Yes (since v1.12 / Spark 3.2) |
| KMS client interface | `pe.KmsClient` (Python) | `KmsClient` (Java interface) |
| AWS KMS client included? | **No** — we wrote our own | **No** — none exists |

Both libraries define a KMS client **interface** but ship no AWS implementation.

- On the Python side, we solved this by writing `AwsKmsClient` (70 lines of
  boto3 code). It plugs into PyArrow and calls `kms:Encrypt` / `kms:Decrypt`.

- On the Java side, **nobody has written the equivalent**. Not Apache. Not AWS.
  The only implementations that exist are:
  - `InMemoryKMS` — a test mock (explicitly "do not use in production")
  - A HashiCorp Vault example

---

## What This Means for Spark

### What works without any KMS client

Because our files use `plaintext_footer=True`, Spark can:

- **See the schema** (column names and types)
- **Read non-encrypted columns** (`xid`, `balance`) with a normal
  `spark.read.parquet()`

```python
# This works — no keys needed
df = spark.read.parquet("s3://bucket/pme-data/").select("xid", "balance")
```

### What does NOT work

Spark cannot decrypt encrypted columns (`ssn`, `first_name`, `last_name`,
`email`) because it has no way to call AWS KMS to unwrap the DEKs:

```python
# This fails — Spark has no KMS client to unwrap DEKs
df = spark.read.parquet("s3://bucket/pme-data/").select("ssn")
# Error: no crypto factory configured / cannot decrypt column
```

Even with the Spark config pointing to parquet-mr's crypto factory:

```python
spark.conf.set("spark.hadoop.parquet.crypto.factory.class",
               "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")
spark.conf.set("spark.hadoop.parquet.encryption.kms.client.class",
               "???")  # <-- Nothing to put here for AWS KMS
```

There is no class to set for `kms.client.class` because no Java AWS KMS
implementation of `org.apache.parquet.crypto.keytools.KmsClient` exists.

---

## Would encrypting with Spark instead of PyArrow help?

**No.** The KMS client is needed for both encryption (wrap DEK) and decryption
(unwrap DEK). Switching the writer from PyArrow to Spark moves the problem — it
does not eliminate it:

| Approach | Write | Read |
|---|---|---|
| **PyArrow encrypt + PyArrow decrypt** | Python KMS client (works) | Python KMS client (works) |
| **PyArrow encrypt + Spark decrypt** | Python KMS client (works) | Java KMS client (missing) |
| **Spark encrypt + Spark decrypt** | Java KMS client (missing) | Java KMS client (missing) |

The current PyArrow-based pipeline is actually the most complete option.

---

## Workaround: PyArrow Inside PySpark

Since Athena Spark runs PySpark, we can use PyArrow directly within a Spark
session to decrypt and then convert to a Spark DataFrame:

```python
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

# 1. Download encrypted file from S3
boto3.client("s3").download_file(bucket, key, "/tmp/data.parquet")

# 2. Decrypt with PyArrow + our Python KMS client
dec_props = crypto_factory.file_decryption_properties(kms_conn, dec_config)
arrow_table = pq.read_table("/tmp/data.parquet", decryption_properties=dec_props)

# 3. Convert to Spark DataFrame — full Spark SQL from here
df = spark.createDataFrame(arrow_table.to_pandas())
df.createOrReplaceTempView("customer_data")
spark.sql("SELECT * FROM customer_data WHERE balance > 1000").show()
```

This gives us the best of both worlds:
- PyArrow handles PME decryption (with our existing KMS client)
- Spark handles querying, joins, aggregations, and analytics

A working notebook demonstrating this is at
[`notebooks/athena_spark_pme_decrypt.ipynb`](notebooks/athena_spark_pme_decrypt.ipynb).

---

## Options to Solve This Long-Term

| Option | Effort | Trade-off |
|---|---|---|
| **PyArrow-in-PySpark workaround** (current) | Done | Extra download step; single-node decrypt before Spark distributes |
| **Write a Java KMS client for AWS** | Medium | ~100 lines of Java + packaging as JAR; needs classpath access in Athena |
| **Lobby Apache / AWS for a built-in client** | Low effort, long wait | parquet-java has had PME since 2021 with no AWS KMS client shipped |
| **Use AWS Lake Formation or Glue encryption** | Medium | Different encryption model; not column-level PME |

---

## Key Takeaway

PME is a **format-level standard** — the encrypted Parquet files are fully
portable between PyArrow and Spark. The gap is not in the encryption or the
file format. The gap is a **missing ~100-line Java class** that nobody has
written and shipped for AWS KMS.

Until that exists, the PyArrow-in-PySpark bridge is a practical and working
solution.
