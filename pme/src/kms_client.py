"""AwsKmsClient — bridges PyArrow Parquet Modular Encryption to AWS KMS.

This module provides:
    AwsKmsClient        – pe.KmsClient implementation backed by boto3 KMS.
    AwsKmsClientFactory – callable that CryptoFactory uses to create clients.

The KMS client has exactly two jobs:
    wrap_key(key_bytes, master_key_id)   → calls KMS Encrypt, returns wrapped DEK
    unwrap_key(wrapped_key, master_key_id) → calls KMS Decrypt, returns plaintext DEK

It does NOT encrypt/decrypt column data (PyArrow handles that with AES-GCM).
It does NOT control access (IAM policies on the CMK control that).
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Dict, Optional

import boto3
import pyarrow.parquet.encryption as pe

logger = logging.getLogger(__name__)

# Key used inside custom_kms_conf JSON to pass the AWS region
_CONF_KEY_REGION = "region"
# Key used inside custom_kms_conf JSON to pass an optional STS role ARN
_CONF_KEY_ROLE_ARN = "role_arn"


def _parse_kms_conf(kms_connection_config) -> dict:
    """Extract config dict from a KmsConnectionConfig or JSON string.

    PyArrow 23+ passes a KmsConnectionConfig object to the factory
    callback; older versions pass a JSON string.
    """
    if isinstance(kms_connection_config, pe.KmsConnectionConfig):
        return dict(kms_connection_config.custom_kms_conf or {})
    if isinstance(kms_connection_config, str) and kms_connection_config:
        try:
            return json.loads(kms_connection_config)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


class AwsKmsClient(pe.KmsClient):
    """PyArrow KmsClient backed by AWS KMS via boto3.

    Parameters
    ----------
    kms_connection_config
        JSON string or KmsConnectionConfig with ``region`` and optional
        ``role_arn``.
    alias_to_arn : dict, optional
        Mapping of short alias → full KMS ARN. PyArrow's
        EncryptionConfiguration uses aliases (no colons) as key
        identifiers; this dict resolves them to ARNs for KMS API calls.
    """

    def __init__(
        self,
        kms_connection_config,
        alias_to_arn: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__()
        conf = _parse_kms_conf(kms_connection_config)
        self._region: str = conf[_CONF_KEY_REGION]
        self._role_arn: Optional[str] = conf.get(_CONF_KEY_ROLE_ARN)
        self._alias_to_arn: Dict[str, str] = alias_to_arn or {}
        self._kms_client = self._build_kms_client()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_key_id(self, master_key_identifier: str) -> str:
        """Resolve a short alias to a full ARN (or return as-is)."""
        return self._alias_to_arn.get(master_key_identifier, master_key_identifier)

    def _build_kms_client(self):
        """Create a boto3 KMS client, optionally via STS assume-role."""
        if self._role_arn:
            sts = boto3.client("sts", region_name=self._region)
            creds = sts.assume_role(
                RoleArn=self._role_arn,
                RoleSessionName="pme-kms-session",
            )["Credentials"]
            return boto3.client(
                "kms",
                region_name=self._region,
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            )
        return boto3.client("kms", region_name=self._region)

    # ------------------------------------------------------------------
    # pe.KmsClient interface
    # ------------------------------------------------------------------

    def wrap_key(self, key_bytes: bytes, master_key_identifier: str) -> str:
        """Encrypt a DEK with the specified KMS CMK.

        Resolves the alias to a full ARN via ``alias_to_arn`` before
        calling KMS. The wrapped (ciphertext) bytes are returned as a
        base64-encoded string for storage in the Parquet footer.
        """
        key_id = self._resolve_key_id(master_key_identifier)
        response = self._kms_client.encrypt(
            KeyId=key_id,
            Plaintext=key_bytes,
        )
        wrapped = base64.b64encode(response["CiphertextBlob"]).decode("utf-8")
        logger.debug("Wrapped DEK with CMK %s → %s", master_key_identifier, key_id)
        return wrapped

    def unwrap_key(self, wrapped_key: str, master_key_identifier: str) -> bytes:
        """Decrypt a wrapped DEK using the specified KMS CMK.

        Resolves the alias to a full ARN via ``alias_to_arn`` before
        calling KMS. Returns the plaintext DEK bytes.
        """
        key_id = self._resolve_key_id(master_key_identifier)
        ciphertext = base64.b64decode(wrapped_key)
        response = self._kms_client.decrypt(
            CiphertextBlob=ciphertext,
            KeyId=key_id,
        )
        logger.debug("Unwrapped DEK with CMK %s → %s", master_key_identifier, key_id)
        return response["Plaintext"]


class AwsKmsClientFactory:
    """Factory callable for ``CryptoFactory(kms_client_factory)``.

    Usage::

        factory = AwsKmsClientFactory(
            region="us-east-2",
            alias_to_arn=config.alias_to_arn,
        )
        crypto_factory = CryptoFactory(factory)

    Or with RBAC (STS assume-role)::

        factory = AwsKmsClientFactory(
            region="us-east-2",
            alias_to_arn=config.alias_to_arn,
            role_arn="arn:aws:iam::123456789012:role/pme-fraud-analyst",
        )
    """

    def __init__(
        self,
        region: str = "us-east-2",
        alias_to_arn: Optional[Dict[str, str]] = None,
        role_arn: Optional[str] = None,
    ) -> None:
        self._region = region
        self._alias_to_arn = alias_to_arn or {}
        self._role_arn = role_arn

    def __call__(self, kms_connection_config) -> AwsKmsClient:
        """Create an AwsKmsClient, merging factory defaults with the
        per-call *kms_connection_config* from PyArrow.

        PyArrow 23+ passes a KmsConnectionConfig object; older versions
        pass a JSON string. Both are handled.
        """
        conf = _parse_kms_conf(kms_connection_config)

        # Apply factory defaults
        conf.setdefault(_CONF_KEY_REGION, self._region)
        if self._role_arn and _CONF_KEY_ROLE_ARN not in conf:
            conf[_CONF_KEY_ROLE_ARN] = self._role_arn

        return AwsKmsClient(json.dumps(conf), alias_to_arn=self._alias_to_arn)
