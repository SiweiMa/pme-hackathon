"""Integration tests — real AWS KMS + S3.

These tests require:
    - Valid AWS credentials (via env vars or /tmp/credentials)
    - KMS keys: alias/pwe-hackathon-{footer,pci,pii}-key in us-east-2
    - S3 bucket: pwe-hackathon-pme-data-651767347247 in us-east-2

Run with:  python -m pytest tests/test_integration.py -v -m integration
"""

from __future__ import annotations

import csv
import os
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import pytest

from pme.src.config import (
    ColumnGroupConfig,
    KmsKeyConfig,
    PmeConfig,
)
from pme.src.decryption import (
    get_file_metadata,
    read_encrypted_from_s3,
    read_encrypted_parquet,
)
from pme.src.encryption import (
    PARQUET_MAGIC_PLAINTEXT,
    build_kms_connection_config,
    verify_pme_file,
    write_encrypted_parquet,
    write_encrypted_to_s3,
)
from pme.src.kms_client import AwsKmsClientFactory

# ---------------------------------------------------------------------------
# Constants — real AWS resources (us-east-2)
# ---------------------------------------------------------------------------

AWS_REGION = "us-east-2"
ACCOUNT_ID = "651767347247"

FOOTER_KEY_ARN = f"arn:aws:kms:{AWS_REGION}:{ACCOUNT_ID}:alias/pwe-hackathon-footer-key"
PCI_KEY_ARN = f"arn:aws:kms:{AWS_REGION}:{ACCOUNT_ID}:alias/pwe-hackathon-pci-key"
PII_KEY_ARN = f"arn:aws:kms:{AWS_REGION}:{ACCOUNT_ID}:alias/pwe-hackathon-pii-key"

S3_BUCKET = f"pwe-hackathon-pme-data-{ACCOUNT_ID}"
S3_PREFIX = "pme-data"

CSV_PATH = Path(__file__).resolve().parents[2] / "data" / "Hackathon_customer_data.csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_credentials():
    """Load AWS credentials from /tmp/credentials into env vars."""
    cred_path = Path("/tmp/credentials")
    if not cred_path.exists():
        return
    text = cred_path.read_text()
    for line in text.splitlines():
        line = line.strip().replace("\r", "")
        if line.startswith("aws_access_key_id="):
            os.environ["AWS_ACCESS_KEY_ID"] = line.split("=", 1)[1]
        elif line.startswith("aws_secret_access_key="):
            os.environ["AWS_SECRET_ACCESS_KEY"] = line.split("=", 1)[1]
    os.environ.setdefault("AWS_DEFAULT_REGION", AWS_REGION)


def _build_real_config(
    *,
    plaintext_footer: bool = True,
) -> PmeConfig:
    """Build PmeConfig using real AWS KMS ARNs and sample CSV columns."""
    return PmeConfig(
        footer_key=KmsKeyConfig(
            key_arn=FOOTER_KEY_ARN, alias="pwe-hackathon-footer-key"
        ),
        column_groups=[
            ColumnGroupConfig(
                name="pci",
                kms_key=KmsKeyConfig(
                    key_arn=PCI_KEY_ARN, alias="pwe-hackathon-pci-key"
                ),
                columns=["ssn"],
            ),
            ColumnGroupConfig(
                name="pii",
                kms_key=KmsKeyConfig(
                    key_arn=PII_KEY_ARN, alias="pwe-hackathon-pii-key"
                ),
                columns=["first_name", "last_name", "email"],
            ),
        ],
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        region=AWS_REGION,
        plaintext_footer=plaintext_footer,
    )


def _build_factory(config: PmeConfig) -> AwsKmsClientFactory:
    """Build an AwsKmsClientFactory with alias resolution."""
    return AwsKmsClientFactory(
        region=config.region,
        alias_to_arn=config.alias_to_arn,
    )


def _load_sample_csv() -> pa.Table:
    """Load Hackathon_customer_data.csv as a PyArrow table."""
    rows: list[dict] = []
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["balance"] = float(row["balance"])
            rows.append(row)
    return pa.Table.from_pylist(rows)


def _decrypt_parquet(path, config, factory) -> pa.Table:
    """Read back an encrypted Parquet file using real KMS."""
    crypto_factory = pe.CryptoFactory(factory)
    kms_conn = build_kms_connection_config(config)
    dec_config = pe.DecryptionConfiguration(cache_lifetime=timedelta(seconds=60))
    file_dec_props = crypto_factory.file_decryption_properties(kms_conn, dec_config)
    return pq.read_table(str(path), decryption_properties=file_dec_props)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def aws_credentials():
    """Load AWS credentials before any test in this module."""
    _load_credentials()
    # Verify credentials work
    import boto3

    try:
        sts = boto3.client("sts", region_name=AWS_REGION)
        sts.get_caller_identity()
    except Exception as e:
        pytest.skip(f"AWS credentials not available: {e}")


@pytest.fixture
def real_config():
    return _build_real_config()


@pytest.fixture
def real_factory(real_config):
    return _build_factory(real_config)


@pytest.fixture
def sample_csv_table():
    if not CSV_PATH.exists():
        pytest.skip(f"Sample CSV not found: {CSV_PATH}")
    return _load_sample_csv()


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLocalEncryptWithRealKms:
    """Write encrypted Parquet locally using real AWS KMS keys."""

    def test_encrypt_sample_csv(self, tmp_path, sample_csv_table, real_config, real_factory):
        """Encrypt the hackathon CSV with real KMS and verify roundtrip."""
        out = tmp_path / "customer_data_encrypted.parquet"
        write_encrypted_parquet(sample_csv_table, out, real_config, real_factory)

        assert out.exists()
        assert out.stat().st_size > 0
        assert verify_pme_file(out, expect_encrypted_footer=False)

    def test_roundtrip_with_real_kms(
        self, tmp_path, sample_csv_table, real_config, real_factory
    ):
        """Encrypt → decrypt → verify data integrity with real KMS."""
        out = tmp_path / "roundtrip.parquet"
        write_encrypted_parquet(sample_csv_table, out, real_config, real_factory)

        recovered = _decrypt_parquet(out, real_config, real_factory)
        assert recovered.num_rows == sample_csv_table.num_rows
        assert set(recovered.column_names) == set(sample_csv_table.column_names)
        assert recovered.equals(sample_csv_table)

    def test_schema_readable_without_keys(
        self, tmp_path, sample_csv_table, real_config, real_factory
    ):
        """Schema is readable without decryption keys (plaintext footer)."""
        out = tmp_path / "schema_test.parquet"
        write_encrypted_parquet(sample_csv_table, out, real_config, real_factory)

        schema = pq.read_schema(str(out))
        assert set(schema.names) == set(sample_csv_table.column_names)

    def test_raw_read_fails_without_keys(
        self, tmp_path, sample_csv_table, real_config, real_factory
    ):
        """Reading encrypted columns without keys raises an error."""
        out = tmp_path / "no_keys.parquet"
        write_encrypted_parquet(sample_csv_table, out, real_config, real_factory)

        with pytest.raises(Exception):
            pq.read_table(str(out))


@pytest.mark.integration
class TestS3EncryptWithRealKms:
    """Write encrypted Parquet to S3 using real AWS KMS keys."""

    def test_write_to_s3_and_verify(self, sample_csv_table, real_config, real_factory):
        """Write encrypted Parquet to S3 and read it back."""
        s3_uri = write_encrypted_to_s3(
            sample_csv_table,
            real_config,
            real_factory,
            filename="integration_test.parquet",
        )
        assert s3_uri.startswith("s3://")

        # Read back from S3 and verify
        import s3fs

        crypto_factory = pe.CryptoFactory(real_factory)
        kms_conn = build_kms_connection_config(real_config)
        dec_config = pe.DecryptionConfiguration(cache_lifetime=timedelta(seconds=60))
        file_dec_props = crypto_factory.file_decryption_properties(kms_conn, dec_config)

        fs = s3fs.S3FileSystem()
        s3_key = s3_uri.replace("s3://", "")
        with fs.open(s3_key, "rb") as f:
            recovered = pq.read_table(f, decryption_properties=file_dec_props)

        assert recovered.num_rows == sample_csv_table.num_rows
        assert recovered.equals(sample_csv_table)


# ---------------------------------------------------------------------------
# Read pipeline integration tests (uses pme.src.decryption module)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLocalDecryptWithRealKms:
    """Read encrypted Parquet locally using real AWS KMS keys."""

    @pytest.fixture
    def encrypted_file(self, tmp_path, sample_csv_table, real_config, real_factory):
        """Write a pre-encrypted file for read tests."""
        out = tmp_path / "read_test.parquet"
        write_encrypted_parquet(sample_csv_table, out, real_config, real_factory)
        return out

    def test_read_encrypted_parquet_roundtrip(
        self, encrypted_file, sample_csv_table, real_config, real_factory
    ):
        """read_encrypted_parquet() recovers original data with real KMS."""
        recovered = read_encrypted_parquet(
            encrypted_file, real_config, real_factory
        )
        assert recovered.num_rows == sample_csv_table.num_rows
        assert set(recovered.column_names) == set(sample_csv_table.column_names)
        assert recovered.equals(sample_csv_table)

    def test_read_with_column_selection(
        self, encrypted_file, sample_csv_table, real_config, real_factory
    ):
        """columns= parameter filters the result columns."""
        subset = ["xid", "balance"]
        recovered = read_encrypted_parquet(
            encrypted_file, real_config, real_factory, columns=subset
        )
        assert recovered.column_names == subset
        assert recovered.num_rows == sample_csv_table.num_rows

    def test_get_file_metadata_row_count(
        self, encrypted_file, sample_csv_table, real_config, real_factory
    ):
        """get_file_metadata() returns correct row count."""
        metadata = get_file_metadata(encrypted_file, real_config, real_factory)
        assert metadata.num_rows == sample_csv_table.num_rows

    def test_get_file_metadata_schema(
        self, encrypted_file, sample_csv_table, real_config, real_factory
    ):
        """get_file_metadata() returns correct column names."""
        metadata = get_file_metadata(encrypted_file, real_config, real_factory)
        schema = metadata.schema.to_arrow_schema()
        assert set(schema.names) == set(sample_csv_table.column_names)

    def test_read_encrypted_footer(
        self, tmp_path, sample_csv_table, real_factory
    ):
        """read_encrypted_parquet() works with encrypted footer."""
        config = _build_real_config(plaintext_footer=False)
        factory = _build_factory(config)
        out = tmp_path / "enc_footer_read.parquet"
        write_encrypted_parquet(sample_csv_table, out, config, factory)

        recovered = read_encrypted_parquet(out, config, factory)
        assert recovered.equals(sample_csv_table)


@pytest.mark.integration
class TestS3DecryptWithRealKms:
    """Read encrypted Parquet from S3 using real AWS KMS keys."""

    def test_s3_write_then_read(self, sample_csv_table, real_config, real_factory):
        """write_encrypted_to_s3() → read_encrypted_from_s3() roundtrip."""
        filename = "read_integration_test.parquet"
        s3_uri = write_encrypted_to_s3(
            sample_csv_table, real_config, real_factory, filename=filename
        )
        assert s3_uri.startswith("s3://")

        recovered = read_encrypted_from_s3(
            real_config, real_factory, filename=filename
        )
        assert recovered.num_rows == sample_csv_table.num_rows
        assert set(recovered.column_names) == set(sample_csv_table.column_names)
        assert recovered.equals(sample_csv_table)

    def test_s3_read_with_column_selection(
        self, sample_csv_table, real_config, real_factory
    ):
        """read_encrypted_from_s3() with columns= filters result."""
        filename = "read_cols_integration_test.parquet"
        write_encrypted_to_s3(
            sample_csv_table, real_config, real_factory, filename=filename
        )

        subset = ["first_name", "email", "xid"]
        recovered = read_encrypted_from_s3(
            real_config, real_factory, filename=filename, columns=subset
        )
        assert recovered.column_names == subset
        assert recovered.num_rows == sample_csv_table.num_rows
