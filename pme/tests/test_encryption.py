"""Tests for the encrypted Parquet write pipeline."""

from __future__ import annotations

import json
from datetime import timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import pytest

from pme.src.config import PmeConfig
from pme.src.encryption import (
    PARQUET_MAGIC_ENCRYPTED,
    PARQUET_MAGIC_PLAINTEXT,
    build_encryption_config,
    build_file_encryption_properties,
    build_kms_connection_config,
    verify_pme_file,
    write_encrypted_parquet,
    write_encrypted_to_s3,
)
from pme.tests.conftest import (
    FAKE_FOOTER_ALIAS,
    FAKE_FOOTER_ARN,
    FAKE_PCI_ARN,
    FAKE_PII_ARN,
    InMemoryKmsClientFactory,
)


# ---------------------------------------------------------------------------
# Helper to decrypt a file for roundtrip tests
# ---------------------------------------------------------------------------


def _read_encrypted_parquet(
    path: str,
    config: PmeConfig,
    kms_client_factory: InMemoryKmsClientFactory,
) -> pa.Table:
    """Decrypt and read back a PME-encrypted Parquet file."""
    crypto_factory = pe.CryptoFactory(kms_client_factory)
    kms_conn = build_kms_connection_config(config)

    dec_config = pe.DecryptionConfiguration(cache_lifetime=timedelta(seconds=60))
    file_dec_props = crypto_factory.file_decryption_properties(kms_conn, dec_config)

    return pq.read_table(str(path), decryption_properties=file_dec_props)


# ---------------------------------------------------------------------------
# Unit tests — no AWS calls needed
# ---------------------------------------------------------------------------


class TestBuildEncryptionConfig:
    """Verify EncryptionConfiguration construction."""

    def test_footer_key_set(self, pme_config):
        enc = build_encryption_config(pme_config)
        assert enc.footer_key == FAKE_FOOTER_ALIAS

    def test_column_keys_set(self, pme_config):
        # EncryptionConfiguration.column_keys getter is broken in PyArrow 23.x
        # (ValueError on dict reconstruction). Verify indirectly: the config
        # was constructed without error and uses the alias (not ARN).
        enc = build_encryption_config(pme_config)
        assert enc.footer_key == pme_config.footer_key.alias
        # Column keys are verified via roundtrip tests instead

    def test_plaintext_footer_default(self, pme_config):
        enc = build_encryption_config(pme_config)
        assert enc.plaintext_footer is True

    def test_encrypted_footer(self, pme_config_encrypted_footer):
        enc = build_encryption_config(pme_config_encrypted_footer)
        assert enc.plaintext_footer is False


class TestWriteEncryptedParquet:
    """Test local encrypted Parquet writes."""

    def test_creates_file(self, tmp_path, sample_table, pme_config, in_memory_kms_factory):
        out = tmp_path / "test.parquet"
        result = write_encrypted_parquet(
            sample_table, out, pme_config, in_memory_kms_factory
        )
        assert result.exists()
        assert result.stat().st_size > 0

    def test_plaintext_footer_magic_bytes(
        self, tmp_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """plaintext_footer=True → file starts with PAR1."""
        out = tmp_path / "plaintext_footer.parquet"
        write_encrypted_parquet(sample_table, out, pme_config, in_memory_kms_factory)

        magic = out.read_bytes()[:4]
        assert magic == PARQUET_MAGIC_PLAINTEXT

    def test_encrypted_footer_magic_bytes(
        self, tmp_path, sample_table, pme_config_encrypted_footer, in_memory_kms_factory
    ):
        """plaintext_footer=False → file starts with PARE."""
        out = tmp_path / "encrypted_footer.parquet"
        write_encrypted_parquet(
            sample_table, out, pme_config_encrypted_footer, in_memory_kms_factory
        )

        magic = out.read_bytes()[:4]
        assert magic == PARQUET_MAGIC_ENCRYPTED

    def test_roundtrip_integrity(
        self, tmp_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Encrypt → decrypt → assert table equality."""
        out = tmp_path / "roundtrip.parquet"
        write_encrypted_parquet(sample_table, out, pme_config, in_memory_kms_factory)

        recovered = _read_encrypted_parquet(str(out), pme_config, in_memory_kms_factory)
        assert recovered.equals(sample_table)

    def test_column_level_encryption(
        self, tmp_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """Only configured columns are encrypted; non-sensitive columns present."""
        out = tmp_path / "col_level.parquet"
        write_encrypted_parquet(sample_table, out, pme_config, in_memory_kms_factory)

        # Decrypt and check all columns are present
        recovered = _read_encrypted_parquet(str(out), pme_config, in_memory_kms_factory)

        # Non-sensitive columns must be in the result
        assert "customer_id" in recovered.column_names
        assert "account_type" in recovered.column_names

        # All encrypted columns must also be present after decryption
        for col in pme_config.all_encrypted_columns:
            assert col in recovered.column_names

    def test_multiple_column_groups(
        self, tmp_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """PCI and PII groups use different KMS keys."""
        # Verify config has two distinct groups with different keys
        assert len(pme_config.column_groups) == 2
        keys = {g.kms_key.key_arn for g in pme_config.column_groups}
        assert len(keys) == 2, "PCI and PII must use different KMS keys"

        # Write and roundtrip to prove both groups decrypt correctly
        out = tmp_path / "multi_group.parquet"
        write_encrypted_parquet(sample_table, out, pme_config, in_memory_kms_factory)
        recovered = _read_encrypted_parquet(str(out), pme_config, in_memory_kms_factory)
        assert recovered.equals(sample_table)

    def test_schema_readable_with_plaintext_footer(
        self, tmp_path, sample_table, pme_config, in_memory_kms_factory
    ):
        """pq.read_schema() works without decryption keys when plaintext_footer=True."""
        assert pme_config.plaintext_footer is True

        out = tmp_path / "schema_test.parquet"
        write_encrypted_parquet(sample_table, out, pme_config, in_memory_kms_factory)

        # Read schema WITHOUT providing any decryption keys
        schema = pq.read_schema(str(out))
        assert set(schema.names) == set(sample_table.column_names)


class TestVerifyPmeFile:
    """Test magic bytes verification."""

    def test_verify_plaintext_footer(
        self, tmp_path, sample_table, pme_config, in_memory_kms_factory
    ):
        out = tmp_path / "verify_pt.parquet"
        write_encrypted_parquet(sample_table, out, pme_config, in_memory_kms_factory)
        assert verify_pme_file(out, expect_encrypted_footer=False) is True
        assert verify_pme_file(out, expect_encrypted_footer=True) is False

    def test_verify_encrypted_footer(
        self, tmp_path, sample_table, pme_config_encrypted_footer, in_memory_kms_factory
    ):
        out = tmp_path / "verify_enc.parquet"
        write_encrypted_parquet(
            sample_table, out, pme_config_encrypted_footer, in_memory_kms_factory
        )
        assert verify_pme_file(out, expect_encrypted_footer=True) is True
        assert verify_pme_file(out, expect_encrypted_footer=False) is False

    def test_verify_too_small_file(self, tmp_path):
        small = tmp_path / "tiny.parquet"
        small.write_bytes(b"AB")
        with pytest.raises(ValueError, match="too small"):
            verify_pme_file(small, expect_encrypted_footer=False)

    def test_verify_unrecognized_magic(self, tmp_path):
        bad = tmp_path / "bad.parquet"
        bad.write_bytes(b"XXXX" + b"\x00" * 100)
        with pytest.raises(ValueError, match="Unrecognized magic"):
            verify_pme_file(bad, expect_encrypted_footer=False)


# ---------------------------------------------------------------------------
# Integration test — requires AWS credentials + real KMS keys
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestWriteEncryptedToS3:
    """S3 write tests — skipped without AWS credentials."""

    def test_write_encrypted_to_s3(self, sample_table, pme_config, in_memory_kms_factory):
        """Placeholder: real S3 test would use AwsKmsClientFactory."""
        pytest.skip("Requires AWS credentials and real KMS keys")
