#!/usr/bin/env python3
"""Demo: encrypt hackathon sample CSV with real AWS KMS and upload to S3.

Usage:
    cd pme && python demo_encrypt.py

This script:
    1. Loads Hackathon_customer_data.csv as a PyArrow table
    2. Encrypts it with column-level PME (PCI + PII groups)
    3. Writes encrypted Parquet locally and to S3
    4. Demonstrates that encrypted columns are unreadable without keys
    5. Decrypts and shows the data round-trips perfectly
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

# Ensure pme package is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pme.src.config import ColumnGroupConfig, KmsKeyConfig, PmeConfig
from pme.src.encryption import (
    PARQUET_MAGIC_ENCRYPTED,
    PARQUET_MAGIC_PLAINTEXT,
    build_kms_connection_config,
    verify_pme_file,
    write_encrypted_parquet,
    write_encrypted_to_s3,
)
from pme.src.kms_client import AwsKmsClientFactory

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION = "us-east-2"
ACCOUNT_ID = "651767347247"
S3_BUCKET = f"pwe-hackathon-pme-data-{ACCOUNT_ID}"

CONFIG = PmeConfig(
    footer_key=KmsKeyConfig(
        key_arn=f"arn:aws:kms:{AWS_REGION}:{ACCOUNT_ID}:alias/pwe-hackathon-footer-key",
        alias="pwe-hackathon-footer-key",
    ),
    column_groups=[
        ColumnGroupConfig(
            name="pci",
            kms_key=KmsKeyConfig(
                key_arn=f"arn:aws:kms:{AWS_REGION}:{ACCOUNT_ID}:alias/pwe-hackathon-pci-key",
                alias="pwe-hackathon-pci-key",
            ),
            columns=["ssn"],
        ),
        ColumnGroupConfig(
            name="pii",
            kms_key=KmsKeyConfig(
                key_arn=f"arn:aws:kms:{AWS_REGION}:{ACCOUNT_ID}:alias/pwe-hackathon-pii-key",
                alias="pwe-hackathon-pii-key",
            ),
            columns=["first_name", "last_name", "email"],
        ),
    ],
    s3_bucket=S3_BUCKET,
    s3_prefix="pme-data",
    region=AWS_REGION,
    plaintext_footer=True,
)

CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "Hackathon_customer_data.csv"
LOCAL_OUTPUT = Path(__file__).resolve().parent / "output"


def load_credentials():
    """Load AWS credentials from /tmp/credentials."""
    cred_path = Path("/tmp/credentials")
    if not cred_path.exists():
        print("ERROR: /tmp/credentials not found")
        sys.exit(1)
    text = cred_path.read_text()
    for line in text.splitlines():
        line = line.strip().replace("\r", "")
        if line.startswith("aws_access_key_id="):
            os.environ["AWS_ACCESS_KEY_ID"] = line.split("=", 1)[1]
        elif line.startswith("aws_secret_access_key="):
            os.environ["AWS_SECRET_ACCESS_KEY"] = line.split("=", 1)[1]
    os.environ["AWS_DEFAULT_REGION"] = AWS_REGION


def load_csv(path: Path) -> pa.Table:
    """Load CSV into a PyArrow table."""
    rows: list[dict] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["balance"] = float(row["balance"])
            rows.append(row)
    table = pa.Table.from_pylist(rows)
    return table


def print_section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def main():
    load_credentials()

    factory = AwsKmsClientFactory(
        region=CONFIG.region,
        alias_to_arn=CONFIG.alias_to_arn,
    )

    # ----- Load sample data -----
    print_section("1. Loading sample data")
    table = load_csv(CSV_PATH)
    print(f"Loaded {table.num_rows} rows, {table.num_columns} columns from CSV")
    print(f"Columns: {table.column_names}")
    print(f"\nFirst 5 rows (plaintext):")
    print(table.slice(0, 5).to_pandas().to_string(index=False))

    # ----- Encryption config summary -----
    print_section("2. Encryption configuration")
    print(f"Region:           {CONFIG.region}")
    print(f"Footer key:       {CONFIG.footer_key.alias}")
    print(f"Plaintext footer: {CONFIG.plaintext_footer}")
    print(f"Algorithm:        {CONFIG.encryption_algorithm.value}")
    for g in CONFIG.column_groups:
        print(f"  [{g.name.upper()}] key={g.kms_key.alias}  columns={g.columns}")
    non_encrypted = [
        c for c in table.column_names if c not in CONFIG.all_encrypted_columns
    ]
    print(f"  [CLEAR] columns={non_encrypted}")

    # ----- Write encrypted Parquet locally -----
    print_section("3. Writing encrypted Parquet (local)")
    LOCAL_OUTPUT.mkdir(exist_ok=True)
    local_path = LOCAL_OUTPUT / "customer_data_encrypted.parquet"
    write_encrypted_parquet(table, local_path, CONFIG, factory)
    size_kb = local_path.stat().st_size / 1024
    print(f"Written to: {local_path}")
    print(f"File size:  {size_kb:.1f} KB")

    # ----- Verify magic bytes -----
    magic = local_path.read_bytes()[:4]
    print(f"Magic bytes: {magic} ({'PAR1 = plaintext footer' if magic == PARQUET_MAGIC_PLAINTEXT else 'PARE = encrypted footer'})")
    assert verify_pme_file(local_path, expect_encrypted_footer=False)

    # ----- Show schema is readable without keys -----
    print_section("4. Schema discovery (no decryption keys needed)")
    schema = pq.read_schema(str(local_path))
    print(f"Schema readable without keys: YES")
    print(f"Column names: {schema.names}")
    for field in schema:
        print(f"  {field.name}: {field.type}")

    # ----- Show that reading data fails without keys -----
    print_section("5. Reading encrypted data WITHOUT keys")
    try:
        pq.read_table(str(local_path))
        print("ERROR: should have raised an exception!")
    except Exception as e:
        err_msg = str(e)
        if len(err_msg) > 120:
            err_msg = err_msg[:120] + "..."
        print(f"Correctly blocked: {type(e).__name__}: {err_msg}")

    # ----- Decrypt and show roundtrip -----
    print_section("6. Decrypting with authorized keys")
    crypto_factory = pe.CryptoFactory(factory)
    kms_conn = build_kms_connection_config(CONFIG)
    dec_config = pe.DecryptionConfiguration(cache_lifetime=timedelta(seconds=60))
    file_dec_props = crypto_factory.file_decryption_properties(kms_conn, dec_config)
    recovered = pq.read_table(str(local_path), decryption_properties=file_dec_props)

    print(f"Decrypted {recovered.num_rows} rows, {recovered.num_columns} columns")
    print(f"\nFirst 5 rows (decrypted):")
    print(recovered.slice(0, 5).to_pandas().to_string(index=False))

    match = recovered.equals(table)
    print(f"\nRoundtrip integrity check: {'PASS' if match else 'FAIL'}")

    # ----- Upload to S3 -----
    print_section("7. Uploading encrypted Parquet to S3")
    s3_uri = write_encrypted_to_s3(
        table, CONFIG, factory, filename="customer_data_encrypted.parquet"
    )
    print(f"Uploaded to: {s3_uri}")

    # ----- Summary -----
    print_section("Summary")
    print(f"  Rows encrypted:       {table.num_rows}")
    print(f"  PCI columns (KMS):    {CONFIG.column_groups[0].columns}")
    print(f"  PII columns (KMS):    {CONFIG.column_groups[1].columns}")
    print(f"  Clear columns:        {non_encrypted}")
    print(f"  Local file:           {local_path}")
    print(f"  S3 location:          {s3_uri}")
    print(f"  Roundtrip integrity:  {'PASS' if match else 'FAIL'}")
    print()


if __name__ == "__main__":
    main()
