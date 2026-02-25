#!/usr/bin/env python3
"""Demo: full write → read pipeline with Hackathon_customer_data.csv.

Uses InMemoryKmsClient (no AWS credentials required).
Run from the repo root:
    python -m pme.demo_read_pipeline
"""

from __future__ import annotations

import csv
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pme.src.config import ColumnGroupConfig, KmsKeyConfig, PmeConfig
from pme.src.decryption import (
    get_file_metadata,
    read_encrypted_parquet,
    read_with_partial_access,
)
from pme.src.encryption import verify_pme_file, write_encrypted_parquet
from pme.tests.conftest import (
    FAKE_FOOTER_ARN,
    FAKE_PCI_ALIAS,
    FAKE_PCI_ARN,
    FAKE_PII_ALIAS,
    FAKE_PII_ARN,
    InMemoryKmsClientFactory,
)

CSV_PATH = Path(__file__).resolve().parents[1] / "Hackathon_customer_data.csv"

SEPARATOR = "=" * 72


def load_csv() -> pa.Table:
    rows: list[dict] = []
    with open(CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            row["balance"] = float(row["balance"])
            rows.append(row)
    return pa.Table.from_pylist(rows)


def build_config() -> PmeConfig:
    return PmeConfig(
        footer_key=KmsKeyConfig(key_arn=FAKE_FOOTER_ARN, alias="pme-footer-key"),
        column_groups=[
            ColumnGroupConfig(
                name="pci",
                kms_key=KmsKeyConfig(key_arn=FAKE_PCI_ARN, alias=FAKE_PCI_ALIAS),
                columns=["ssn"],
            ),
            ColumnGroupConfig(
                name="pii",
                kms_key=KmsKeyConfig(key_arn=FAKE_PII_ARN, alias=FAKE_PII_ALIAS),
                columns=["first_name", "last_name", "email"],
            ),
        ],
        region="us-east-1",
    )


def main():
    print(SEPARATOR)
    print("PME PIPELINE DEMO — Hackathon_customer_data.csv")
    print(SEPARATOR)

    # ── 1. Load CSV ──────────────────────────────────────────────────────
    print("\n[1] Loading CSV...")
    table = load_csv()
    print(f"    Loaded {table.num_rows} rows, {table.num_columns} columns")
    print(f"    Columns: {table.column_names}")
    print(f"    Schema:\n{table.schema}")
    print(f"\n    First 5 rows:")
    df = table.to_pandas()
    print(df.head().to_string(index=False))

    # ── 2. Configure encryption ──────────────────────────────────────────
    config = build_config()
    factory = InMemoryKmsClientFactory()

    print(f"\n[2] Encryption config:")
    print(f"    Footer key alias: {config.footer_key.alias}")
    for cg in config.column_groups:
        print(f"    {cg.name.upper()} key alias: {cg.kms_key.alias}  →  columns: {cg.columns}")
    print(f"    Non-encrypted columns: {[c for c in table.column_names if c not in config.all_encrypted_columns]}")
    print(f"    Algorithm: {config.encryption_algorithm.value}")
    print(f"    Plaintext footer: {config.plaintext_footer}")

    # ── 3. Write encrypted Parquet ───────────────────────────────────────
    with tempfile.TemporaryDirectory() as tmp:
        out_path = Path(tmp) / "customer_data_encrypted.parquet"

        print(f"\n[3] Writing encrypted Parquet...")
        write_encrypted_parquet(table, out_path, config, factory)
        file_size = out_path.stat().st_size
        print(f"    File: {out_path.name}")
        print(f"    Size: {file_size:,} bytes")

        is_plaintext = verify_pme_file(out_path, expect_encrypted_footer=False)
        magic = out_path.read_bytes()[:4]
        print(f"    Magic bytes: {magic} ({'plaintext footer' if is_plaintext else 'encrypted footer'})")

        # ── 4. Read schema without keys ──────────────────────────────────
        print(f"\n[4] Reading schema WITHOUT decryption keys (plaintext footer)...")
        schema = pq.read_schema(str(out_path))
        print(f"    Schema columns: {schema.names}")
        print(f"    (No KMS keys needed — footer is plaintext)")

        # ── 5. Read without keys fails ───────────────────────────────────
        print(f"\n[5] Attempting to read DATA without decryption keys...")
        try:
            pq.read_table(str(out_path))
            print("    ERROR: read succeeded without keys (unexpected)")
        except Exception as e:
            print(f"    Correctly failed: {type(e).__name__}")

        # ── 6. Read with full access (fraud analyst) ─────────────────────
        print(f"\n[6] Reading with FULL ACCESS (fraud analyst role)...")
        recovered = read_encrypted_parquet(out_path, config, factory)
        print(f"    Recovered {recovered.num_rows} rows, {recovered.num_columns} columns")
        print(f"    Data matches original: {recovered.equals(table)}")
        print(f"\n    First 5 rows:")
        print(recovered.to_pandas().head().to_string(index=False))

        # ── 7. Read with column selection ────────────────────────────────
        print(f"\n[7] Reading with columns=['xid', 'balance'] (full access)...")
        subset = read_encrypted_parquet(out_path, config, factory, columns=["xid", "balance"])
        print(f"    Recovered {subset.num_rows} rows, {subset.num_columns} columns")
        print(f"    Columns: {subset.column_names}")
        print(f"\n    First 5 rows:")
        print(subset.to_pandas().head().to_string(index=False))

        # ── 8. File metadata ─────────────────────────────────────────────
        print(f"\n[8] File metadata (via get_file_metadata)...")
        metadata = get_file_metadata(out_path, config, factory)
        print(f"    num_rows: {metadata.num_rows}")
        print(f"    num_row_groups: {metadata.num_row_groups}")
        print(f"    num_columns: {metadata.num_columns}")
        print(f"    created_by: {metadata.created_by}")

        # ── 9. RBAC — Marketing analyst (PCI denied → nulled) ────────────
        print(f"\n[9] RBAC: Marketing analyst (PCI key DENIED → ssn nulled)...")
        marketing = read_with_partial_access(
            out_path, config, factory,
            denied_key_aliases=frozenset({FAKE_PCI_ALIAS}),
        )
        print(f"    Recovered {marketing.num_rows} rows, {marketing.num_columns} columns")
        pci_cols = [c for g in config.column_groups if g.kms_key.alias == FAKE_PCI_ALIAS for c in g.columns]
        print(f"    Nulled columns (PCI): {pci_cols}")
        print(f"\n    First 5 rows:")
        print(marketing.to_pandas().head().to_string(index=False))

        # ── 10. RBAC — Junior analyst (PCI + PII denied → nulled) ───────
        print(f"\n[10] RBAC: Junior analyst (PCI + PII keys DENIED → nulled)...")
        junior = read_with_partial_access(
            out_path, config, factory,
            denied_key_aliases=frozenset({FAKE_PCI_ALIAS, FAKE_PII_ALIAS}),
        )
        print(f"    Recovered {junior.num_rows} rows, {junior.num_columns} columns")
        pii_cols = [c for g in config.column_groups if g.kms_key.alias == FAKE_PII_ALIAS for c in g.columns]
        print(f"    Nulled columns (PCI+PII): {pci_cols + pii_cols}")
        print(f"\n    First 5 rows:")
        print(junior.to_pandas().head().to_string(index=False))

        # ── 11. Encrypted footer mode ────────────────────────────────────
        print(f"\n[11] Encrypted footer mode (plaintext_footer=False)...")
        config_enc = PmeConfig(
            footer_key=config.footer_key,
            column_groups=config.column_groups,
            region=config.region,
            plaintext_footer=False,
        )
        out_enc = Path(tmp) / "encrypted_footer.parquet"
        write_encrypted_parquet(table, out_enc, config_enc, factory)
        magic_enc = out_enc.read_bytes()[:4]
        print(f"    Magic bytes: {magic_enc} ({'encrypted footer' if magic_enc == b'PARE' else 'plaintext footer'})")
        print(f"    File size: {out_enc.stat().st_size:,} bytes")

        try:
            pq.read_schema(str(out_enc))
            print("    Schema without keys: readable (unexpected)")
        except Exception:
            print("    Schema without keys: NOT readable (footer is encrypted)")

        recovered_enc = read_encrypted_parquet(out_enc, config_enc, factory)
        print(f"    Read with keys: {recovered_enc.num_rows} rows, data matches: {recovered_enc.equals(table)}")

    print(f"\n{SEPARATOR}")
    print("DEMO COMPLETE")
    print(SEPARATOR)


if __name__ == "__main__":
    main()
