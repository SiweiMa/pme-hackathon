"""Tests for the encrypted Parquet read pipeline and RBAC access control."""

from __future__ import annotations

from datetime import timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import pytest

from pme.src.decryption import (
    build_decryption_config,
    get_file_metadata,
    read_encrypted_parquet,
    read_with_partial_access,
)
from pme.src.encryption import write_encrypted_parquet
from pme.tests.conftest import (
    FAKE_PCI_ALIAS,
    FAKE_PII_ALIAS,
)


# ---------------------------------------------------------------------------
# TestBuildDecryptionConfig
# ---------------------------------------------------------------------------


class TestBuildDecryptionConfig:
    """Verify DecryptionConfiguration construction."""

    def test_creates_decryption_config(self, pme_config):
        dec = build_decryption_config(pme_config)
        assert isinstance(dec, pe.DecryptionConfiguration)

    def test_cache_lifetime_from_config(self, pme_config):
        dec = build_decryption_config(pme_config)
        assert dec.cache_lifetime == timedelta(
            seconds=pme_config.cache_lifetime_seconds
        )


# ---------------------------------------------------------------------------
# TestReadEncryptedParquet
# ---------------------------------------------------------------------------


class TestReadEncryptedParquet:
    """Test local encrypted Parquet reads."""

    def test_roundtrip_integrity(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Encrypt → decrypt → assert table equality."""
        recovered = read_encrypted_parquet(
            encrypted_parquet_path, pme_config, in_memory_kms_factory
        )
        assert recovered.equals(sample_table)

    def test_preserves_column_order(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Column names match original order."""
        recovered = read_encrypted_parquet(
            encrypted_parquet_path, pme_config, in_memory_kms_factory
        )
        assert recovered.column_names == sample_table.column_names

    def test_columns_parameter_selects_subset(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """columns= filters result to requested columns."""
        subset = ["customer_id", "first_name"]
        recovered = read_encrypted_parquet(
            encrypted_parquet_path, pme_config, in_memory_kms_factory, columns=subset
        )
        assert recovered.column_names == subset
        assert recovered.num_rows == sample_table.num_rows

    def test_reads_plaintext_footer(
        self, tmp_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Works with plaintext_footer=True."""
        assert pme_config.plaintext_footer is True
        out = tmp_path / "pt_footer.parquet"
        write_encrypted_parquet(sample_table, out, pme_config, in_memory_kms_factory)

        recovered = read_encrypted_parquet(
            out, pme_config, in_memory_kms_factory
        )
        assert recovered.equals(sample_table)

    def test_reads_encrypted_footer(
        self,
        tmp_path,
        sample_table,
        pme_config_encrypted_footer,
        in_memory_kms_factory,
    ):
        """Works with plaintext_footer=False."""
        assert pme_config_encrypted_footer.plaintext_footer is False
        out = tmp_path / "enc_footer.parquet"
        write_encrypted_parquet(
            sample_table, out, pme_config_encrypted_footer, in_memory_kms_factory
        )

        recovered = read_encrypted_parquet(
            out, pme_config_encrypted_footer, in_memory_kms_factory
        )
        assert recovered.equals(sample_table)

    def test_schema_and_types_preserved(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """dtypes survive the encrypt → decrypt roundtrip."""
        recovered = read_encrypted_parquet(
            encrypted_parquet_path, pme_config, in_memory_kms_factory
        )
        assert recovered.schema.equals(sample_table.schema)


# ---------------------------------------------------------------------------
# TestRbacAccessControl
# ---------------------------------------------------------------------------


class TestRbacAccessControl:
    """Validate partial-access RBAC with null masking."""

    def test_fraud_analyst_reads_all_columns(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Full access (no denied keys) — all data matches original."""
        recovered = read_with_partial_access(
            encrypted_parquet_path, pme_config, in_memory_kms_factory
        )
        assert recovered.equals(sample_table)

    def test_marketing_analyst_pci_nulled(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """PCI denied — PCI columns are null, PII + non-sensitive intact."""
        recovered = read_with_partial_access(
            encrypted_parquet_path,
            pme_config,
            in_memory_kms_factory,
            denied_key_aliases=frozenset({FAKE_PCI_ALIAS}),
        )
        assert recovered.num_rows == sample_table.num_rows
        assert recovered.column_names == sample_table.column_names

        # PCI columns (ssn, pan, card_number) must be all null
        for col in ["ssn", "pan", "card_number"]:
            assert recovered.column(col).null_count == recovered.num_rows

        # PII columns must match original
        for col in ["first_name", "last_name", "email", "phone"]:
            assert recovered.column(col).equals(sample_table.column(col))

        # Non-sensitive columns must match original
        for col in ["customer_id", "account_type"]:
            assert recovered.column(col).equals(sample_table.column(col))

    def test_junior_analyst_pci_and_pii_nulled(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """PCI+PII denied — only non-sensitive columns have data."""
        recovered = read_with_partial_access(
            encrypted_parquet_path,
            pme_config,
            in_memory_kms_factory,
            denied_key_aliases=frozenset({FAKE_PCI_ALIAS, FAKE_PII_ALIAS}),
        )
        assert recovered.num_rows == sample_table.num_rows
        assert recovered.column_names == sample_table.column_names

        # PCI + PII columns must be all null
        for col in ["ssn", "pan", "card_number", "first_name", "last_name", "email", "phone"]:
            assert recovered.column(col).null_count == recovered.num_rows

        # Non-sensitive columns must match original
        for col in ["customer_id", "account_type"]:
            assert recovered.column(col).equals(sample_table.column(col))

    def test_no_denied_keys_returns_full_data(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Empty denied set → identical to read_encrypted_parquet."""
        recovered = read_with_partial_access(
            encrypted_parquet_path,
            pme_config,
            in_memory_kms_factory,
            denied_key_aliases=frozenset(),
        )
        assert recovered.equals(sample_table)

    def test_column_order_preserved(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Column order matches original even when some are nulled."""
        recovered = read_with_partial_access(
            encrypted_parquet_path,
            pme_config,
            in_memory_kms_factory,
            denied_key_aliases=frozenset({FAKE_PCI_ALIAS}),
        )
        assert recovered.column_names == sample_table.column_names

    def test_columns_filter_with_denied(
        self, encrypted_parquet_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """columns= filter works together with denied keys."""
        recovered = read_with_partial_access(
            encrypted_parquet_path,
            pme_config,
            in_memory_kms_factory,
            denied_key_aliases=frozenset({FAKE_PCI_ALIAS}),
            columns=["ssn", "customer_id"],
        )
        assert recovered.column_names == ["ssn", "customer_id"]
        assert recovered.column("ssn").null_count == recovered.num_rows
        assert recovered.column("customer_id").equals(sample_table.column("customer_id"))

    def test_all_roles_can_read_schema_with_plaintext_footer(
        self, encrypted_parquet_path, sample_table, pme_config,
    ):
        """pq.read_schema() works without keys when plaintext_footer=True."""
        assert pme_config.plaintext_footer is True
        schema = pq.read_schema(str(encrypted_parquet_path))
        assert set(schema.names) == set(sample_table.column_names)


# ---------------------------------------------------------------------------
# TestGetFileMetadata
# ---------------------------------------------------------------------------


class TestGetFileMetadata:
    """Test file metadata retrieval."""

    def test_returns_row_count(
        self,
        encrypted_parquet_path,
        sample_table,
        pme_config,
        in_memory_kms_factory,
    ):
        """metadata.num_rows matches the original table."""
        metadata = get_file_metadata(
            encrypted_parquet_path, pme_config, in_memory_kms_factory
        )
        assert metadata.num_rows == sample_table.num_rows

    def test_returns_schema(
        self,
        encrypted_parquet_path,
        sample_table,
        pme_config,
        in_memory_kms_factory,
    ):
        """metadata schema matches original column names."""
        metadata = get_file_metadata(
            encrypted_parquet_path, pme_config, in_memory_kms_factory
        )
        schema = metadata.schema.to_arrow_schema()
        assert set(schema.names) == set(sample_table.column_names)
