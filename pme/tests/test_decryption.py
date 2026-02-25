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
)
from pme.src.encryption import write_encrypted_parquet
from pme.tests.conftest import (
    FAKE_PCI_ALIAS,
    FAKE_PII_ALIAS,
    InMemoryKmsClientFactory,
    RestrictedInMemoryKmsClientFactory,
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
    """Validate RBAC model via restricted KMS clients."""

    def test_fraud_analyst_reads_all_columns(
        self, encrypted_parquet_path, sample_table, pme_config, fraud_analyst_factory
    ):
        """Full access succeeds — all data matches."""
        recovered = read_encrypted_parquet(
            encrypted_parquet_path, pme_config, fraud_analyst_factory
        )
        assert recovered.equals(sample_table)

    def test_marketing_analyst_denied(
        self, encrypted_parquet_path, pme_config, marketing_analyst_factory
    ):
        """PCI denied → raises PermissionError."""
        with pytest.raises(PermissionError):
            read_encrypted_parquet(
                encrypted_parquet_path, pme_config, marketing_analyst_factory
            )

    def test_junior_analyst_denied(
        self, encrypted_parquet_path, pme_config, junior_analyst_factory
    ):
        """PCI+PII denied → raises PermissionError."""
        with pytest.raises(PermissionError):
            read_encrypted_parquet(
                encrypted_parquet_path, pme_config, junior_analyst_factory
            )

    def test_error_identifies_denied_key(
        self, encrypted_parquet_path, pme_config, marketing_analyst_factory
    ):
        """Exception message contains the denied key alias."""
        with pytest.raises(PermissionError, match=FAKE_PCI_ALIAS):
            read_encrypted_parquet(
                encrypted_parquet_path, pme_config, marketing_analyst_factory
            )

    def test_all_roles_can_read_schema_with_plaintext_footer(
        self,
        encrypted_parquet_path,
        sample_table,
        pme_config,
        junior_analyst_factory,
    ):
        """pq.read_schema() works for all roles when plaintext_footer=True."""
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
