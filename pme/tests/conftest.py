"""Shared test fixtures for PME tests.

The centrepiece is InMemoryKmsClient — a deterministic, no-AWS KMS client
that can be used in any test that needs encryption/decryption without
making real AWS calls.
"""

from __future__ import annotations

import base64
import json
import os
from datetime import timedelta
from typing import Dict, List

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
import pytest

from pme.src.config import (
    PmeConfig,
    KmsKeyConfig,
    ColumnGroupConfig,
    EncryptionAlgorithm,
    default_config,
)


# ---------------------------------------------------------------------------
# InMemoryKmsClient — test double for AwsKmsClient
# ---------------------------------------------------------------------------

class InMemoryKmsClient(pe.KmsClient):
    """A fake KMS client that wraps/unwraps DEKs using simple base64
    encoding (prefixed with the master key ID so unwrap can verify).

    No AWS calls are made. Suitable for unit tests.

    Accepts either a JSON string or a KmsConnectionConfig object
    (PyArrow 23+ passes the object directly to the factory callback).
    """

    WRAP_PREFIX = b"INMEM:"

    def __init__(self, kms_connection_config=None) -> None:
        super().__init__()
        if kms_connection_config is None:
            self._conf: Dict = {}
        elif isinstance(kms_connection_config, pe.KmsConnectionConfig):
            self._conf = dict(kms_connection_config.custom_kms_conf or {})
        elif isinstance(kms_connection_config, str):
            self._conf = json.loads(kms_connection_config) if kms_connection_config else {}
        else:
            self._conf = {}

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
    """Factory that creates InMemoryKmsClient instances.

    PyArrow 23+ passes a KmsConnectionConfig object to the factory
    callback (not a JSON string).
    """

    def __call__(self, kms_connection_config) -> InMemoryKmsClient:
        return InMemoryKmsClient(kms_connection_config)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FAKE_FOOTER_ARN = "arn:aws:kms:us-east-1:123456789012:key/footer-key-id"
FAKE_PCI_ARN = "arn:aws:kms:us-east-1:123456789012:key/pci-key-id"
FAKE_PII_ARN = "arn:aws:kms:us-east-1:123456789012:key/pii-key-id"

# Aliases used in EncryptionConfiguration (no colons — PyArrow requirement)
FAKE_FOOTER_ALIAS = "pme-footer-key"
FAKE_PCI_ALIAS = "pme-pci-key"
FAKE_PII_ALIAS = "pme-pii-key"


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


@pytest.fixture
def pme_config_encrypted_footer() -> PmeConfig:
    """Return a PmeConfig with plaintext_footer=False (encrypted footer)."""
    config = default_config(
        footer_key_arn=FAKE_FOOTER_ARN,
        pci_key_arn=FAKE_PCI_ARN,
        pii_key_arn=FAKE_PII_ARN,
    )
    config.plaintext_footer = False
    return config


@pytest.fixture
def sample_table() -> pa.Table:
    """Return a small PyArrow table with PCI, PII, and non-sensitive columns."""
    return pa.table(
        {
            # PCI columns
            "ssn": ["123-45-6789", "987-65-4321"],
            "pan": ["4111111111111111", "5500000000000004"],
            "card_number": ["4111-1111-1111-1111", "5500-0000-0000-0004"],
            # PII columns
            "first_name": ["Alice", "Bob"],
            "last_name": ["Smith", "Jones"],
            "email": ["alice@example.com", "bob@example.com"],
            "phone": ["555-0100", "555-0200"],
            # Non-sensitive columns
            "customer_id": [1001, 1002],
            "account_type": ["checking", "savings"],
        }
    )


@pytest.fixture
def crypto_factory(in_memory_kms_factory) -> pe.CryptoFactory:
    """Return a CryptoFactory backed by InMemoryKmsClient."""
    return pe.CryptoFactory(in_memory_kms_factory)


@pytest.fixture
def kms_conn_config() -> pe.KmsConnectionConfig:
    """Return a KmsConnectionConfig for test use."""
    kms_conn = pe.KmsConnectionConfig()
    kms_conn.custom_kms_conf = {"region": "us-east-1"}
    return kms_conn
