#!/usr/bin/env python3
"""Inspect an encrypted Parquet file: show what's visible with and without keys."""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pme.src.config import ColumnGroupConfig, KmsKeyConfig, PmeConfig
from pme.src.encryption import build_kms_connection_config
from pme.src.kms_client import AwsKmsClientFactory

REGION = "us-east-2"
ACCT = "651767347247"

CONFIG = PmeConfig(
    footer_key=KmsKeyConfig(
        key_arn=f"arn:aws:kms:{REGION}:{ACCT}:alias/pwe-hackathon-footer-key",
        alias="pwe-hackathon-footer-key",
    ),
    column_groups=[
        ColumnGroupConfig(
            name="pci",
            kms_key=KmsKeyConfig(
                key_arn=f"arn:aws:kms:{REGION}:{ACCT}:alias/pwe-hackathon-pci-key",
                alias="pwe-hackathon-pci-key",
            ),
            columns=["ssn"],
        ),
        ColumnGroupConfig(
            name="pii",
            kms_key=KmsKeyConfig(
                key_arn=f"arn:aws:kms:{REGION}:{ACCT}:alias/pwe-hackathon-pii-key",
                alias="pwe-hackathon-pii-key",
            ),
            columns=["first_name", "last_name", "email"],
        ),
    ],
    region=REGION,
    s3_bucket="unused",
    plaintext_footer=True,
)

FILE = Path(__file__).resolve().parent / "output" / "customer_data_encrypted.parquet"


def load_credentials():
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
    os.environ["AWS_DEFAULT_REGION"] = REGION


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def main():
    load_credentials()
    path = str(FILE)

    # ---- WITHOUT KEYS ----
    section("WITHOUT decryption keys")

    print("Schema (readable via plaintext footer):")
    schema = pq.read_schema(path)
    for f in schema:
        print(f"  {f.name}: {f.type}")

    print("\nParquet metadata:")
    meta = pq.read_metadata(path)
    print(f"  Rows:       {meta.num_rows}")
    print(f"  Row groups: {meta.num_row_groups}")
    print(f"  Columns:    {meta.num_columns}")

    print("\nAttempting to read data...")
    try:
        pq.read_table(path)
        print("  ERROR: should have been blocked!")
    except Exception as e:
        print(f"  BLOCKED: {e}")

    # ---- WITH KEYS ----
    section("WITH decryption keys (full access)")

    factory = AwsKmsClientFactory(region=REGION, alias_to_arn=CONFIG.alias_to_arn)
    crypto = pe.CryptoFactory(factory)
    kms_conn = build_kms_connection_config(CONFIG)
    dec_config = pe.DecryptionConfiguration(cache_lifetime=timedelta(seconds=60))
    dec_props = crypto.file_decryption_properties(kms_conn, dec_config)

    table = pq.read_table(path, decryption_properties=dec_props)
    df = table.to_pandas()

    encrypted_cols = CONFIG.all_encrypted_columns
    clear_cols = [c for c in df.columns if c not in encrypted_cols]

    print(f"Encrypted columns (PCI+PII): {encrypted_cols}")
    print(f"Clear columns:               {clear_cols}")
    print(f"\nFirst 10 rows (decrypted):\n")
    print(df.head(10).to_string(index=False))

    # ---- RAW BYTE SCAN ----
    section("Raw byte scan — is plaintext leaking?")

    raw = FILE.read_bytes()
    print(f"File size:   {len(raw):,} bytes")
    magic = raw[:4]
    label = "PAR1 = plaintext footer" if magic == b"PAR1" else "PARE = encrypted footer"
    print(f"Magic bytes: {magic} ({label})")
    print()

    for col in clear_cols:
        val = str(df[col].iloc[0])
        found = val.encode() in raw
        status = "VISIBLE in raw bytes (not encrypted)" if found else "not found"
        print(f'  Clear col "{col}" — value "{val}": {status}')

    for col in encrypted_cols:
        val = str(df[col].iloc[0])
        found = val.encode() in raw
        status = "LEAKED (BUG!)" if found else "NOT visible (encrypted)"
        print(f'  Encrypted col "{col}" — value "{val}": {status}')


if __name__ == "__main__":
    main()
