"""Shared test fixtures for PME tests.

The centrepiece is InMemoryKmsClient — a deterministic, no-AWS KMS client
that can be used in any test that needs encryption/decryption without
making real AWS calls.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Dict

import pyarrow.parquet.encryption as pe
import pytest

from pme.src.config import PmeConfig, KmsKeyConfig, ColumnGroupConfig, default_config


# ---------------------------------------------------------------------------
# InMemoryKmsClient — test double for AwsKmsClient
# ---------------------------------------------------------------------------

class InMemoryKmsClient(pe.KmsClient):
    """A fake KMS client that wraps/unwraps DEKs using simple base64
    encoding (prefixed with the master key ID so unwrap can verify).

    No AWS calls are made. Suitable for unit tests.
    """

    WRAP_PREFIX = b"INMEM:"

    def __init__(self, kms_connection_config: str = "{}") -> None:
        super().__init__()
        self._conf = json.loads(kms_connection_config) if kms_connection_config else {}

    def wrap_key(self, key_bytes: bytes, master_key_identifier: str) -> str:
        payload = self.WRAP_PREFIX + master_key_identifier.encode() + b"|" + key_bytes
        return base64.b64encode(payload).decode("utf-8")

    def unwrap_key(self, wrapped_key: str, master_key_identifier: str) -> bytes:
        payload = base64.b64decode(wrapped_key)
        assert payload.startswith(self.WRAP_PREFIX), "Not wrapped by InMemoryKmsClient"
        remainder = payload[len(self.WRAP_PREFIX):]
        stored_id, key_bytes = remainder.split(b"|", 1)
        assert stored_id.decode() == master_key_identifier, (
            f"Key mismatch: wrapped for {stored_id.decode()!r}, "
            f"unwrap requested with {master_key_identifier!r}"
        )
        return key_bytes


class InMemoryKmsClientFactory:
    """Factory that creates InMemoryKmsClient instances."""

    def __call__(self, kms_connection_config: str) -> InMemoryKmsClient:
        return InMemoryKmsClient(kms_connection_config)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FAKE_FOOTER_ARN = "arn:aws:kms:us-east-1:123456789012:alias/pme-footer-key"
FAKE_PCI_ARN = "arn:aws:kms:us-east-1:123456789012:alias/pme-pci-key"
FAKE_PII_ARN = "arn:aws:kms:us-east-1:123456789012:alias/pme-pii-key"


@pytest.fixture
def in_memory_kms_client() -> InMemoryKmsClient:
    """Return a fresh InMemoryKmsClient."""
    return InMemoryKmsClient()


@pytest.fixture
def in_memory_kms_factory() -> InMemoryKmsClientFactory:
    """Return a factory that creates InMemoryKmsClient instances."""
    return InMemoryKmsClientFactory()


@pytest.fixture
def pme_config() -> PmeConfig:
    """Return a PmeConfig using fake KMS ARNs (for unit tests)."""
    return default_config(
        footer_key_arn=FAKE_FOOTER_ARN,
        pci_key_arn=FAKE_PCI_ARN,
        pii_key_arn=FAKE_PII_ARN,
    )
