"""PME configuration: KMS ARNs, column-key mappings, algorithm settings."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class EncryptionAlgorithm(Enum):
    """Supported PME encryption algorithms."""

    AES_GCM_V1 = "AES_GCM_V1"
    AES_GCM_CTR_V1 = "AES_GCM_CTR_V1"


@dataclass
class KmsKeyConfig:
    """Configuration for a single KMS CMK."""

    key_arn: str
    alias: str


@dataclass
class ColumnGroupConfig:
    """Maps a column group (e.g. 'pci', 'pii') to a KMS key and its columns."""

    name: str
    kms_key: KmsKeyConfig
    columns: List[str]


@dataclass
class PmeConfig:
    """Top-level PME configuration.

    Attributes:
        footer_key: KMS key used to encrypt the Parquet footer.
        column_groups: List of column groups, each mapped to a KMS key.
        s3_bucket: S3 bucket for encrypted Parquet files.
        s3_prefix: S3 key prefix within the bucket.
        region: AWS region for KMS calls.
        encryption_algorithm: AES_GCM_V1 or AES_GCM_CTR_V1.
        plaintext_footer: If True, the footer is signed but readable
            without keys (allows schema discovery).
        cache_lifetime_seconds: How long PyArrow caches DEKs (reduces
            KMS API calls).
        double_wrapping: Whether to double-wrap DEKs (False for hackathon).
    """

    footer_key: KmsKeyConfig
    column_groups: List[ColumnGroupConfig]
    s3_bucket: str = "hackathon-pme-2026"
    s3_prefix: str = "pme-data"
    region: str = "us-east-1"
    encryption_algorithm: EncryptionAlgorithm = EncryptionAlgorithm.AES_GCM_V1
    plaintext_footer: bool = True
    cache_lifetime_seconds: int = 600
    double_wrapping: bool = False

    # ------------------------------------------------------------------
    # Derived helpers
    # ------------------------------------------------------------------

    @property
    def column_key_mapping(self) -> Dict[str, str]:
        """Return {column_name: kms_key_arn} for every encrypted column."""
        mapping: Dict[str, str] = {}
        for group in self.column_groups:
            for col in group.columns:
                mapping[col] = group.kms_key.key_arn
        return mapping

    @property
    def all_encrypted_columns(self) -> List[str]:
        """Return a flat list of all encrypted column names."""
        return [col for group in self.column_groups for col in group.columns]

    @property
    def s3_path(self) -> str:
        """Return the full S3 URI for the data prefix."""
        return f"s3://{self.s3_bucket}/{self.s3_prefix}"


def default_config(
    footer_key_arn: str,
    pci_key_arn: str,
    pii_key_arn: str,
    *,
    region: str = "us-east-1",
    s3_bucket: str = "hackathon-pme-2026",
) -> PmeConfig:
    """Build the default hackathon config from three KMS ARNs."""
    return PmeConfig(
        footer_key=KmsKeyConfig(key_arn=footer_key_arn, alias="pme-footer-key"),
        column_groups=[
            ColumnGroupConfig(
                name="pci",
                kms_key=KmsKeyConfig(key_arn=pci_key_arn, alias="pme-pci-key"),
                columns=["ssn", "pan", "card_number"],
            ),
            ColumnGroupConfig(
                name="pii",
                kms_key=KmsKeyConfig(key_arn=pii_key_arn, alias="pme-pii-key"),
                columns=["first_name", "last_name", "email", "phone"],
            ),
        ],
        region=region,
        s3_bucket=s3_bucket,
    )
