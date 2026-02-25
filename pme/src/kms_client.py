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
from typing import Optional

import boto3
import pyarrow.parquet.encryption as pe

logger = logging.getLogger(__name__)

# Key used inside custom_kms_conf JSON to pass the AWS region
_CONF_KEY_REGION = "region"
# Key used inside custom_kms_conf JSON to pass an optional STS role ARN
_CONF_KEY_ROLE_ARN = "role_arn"


class AwsKmsClient(pe.KmsClient):
    """PyArrow KmsClient backed by AWS KMS via boto3.

    Parameters
    ----------
    kms_connection_config : str
        JSON string with ``region`` and optional ``role_arn``.
        Example: ``{"region": "us-east-1", "role_arn": "arn:aws:iam::..."}``
    """

    def __init__(self, kms_connection_config: str) -> None:
        super().__init__()
        conf = json.loads(kms_connection_config)
        self._region: str = conf[_CONF_KEY_REGION]
        self._role_arn: Optional[str] = conf.get(_CONF_KEY_ROLE_ARN)
        self._kms_client = self._build_kms_client()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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

        PyArrow generates a random DEK and passes it here. We ask KMS to
        encrypt it using the CMK identified by *master_key_identifier*
        (a full KMS ARN). The wrapped (ciphertext) bytes are returned as
        a base64-encoded string so PyArrow can store them as UTF-8 in the
        Parquet footer.
        """
        response = self._kms_client.encrypt(
            KeyId=master_key_identifier,
            Plaintext=key_bytes,
        )
        wrapped = base64.b64encode(response["CiphertextBlob"]).decode("utf-8")
        logger.debug("Wrapped DEK with CMK %s", master_key_identifier)
        return wrapped

    def unwrap_key(self, wrapped_key: str, master_key_identifier: str) -> bytes:
        """Decrypt a wrapped DEK using the specified KMS CMK.

        PyArrow reads the wrapped DEK from the Parquet footer and passes
        it here. We decode the base64 string, send the ciphertext to KMS
        for decryption, and return the plaintext DEK bytes so PyArrow can
        decrypt the column data locally.
        """
        ciphertext = base64.b64decode(wrapped_key)
        response = self._kms_client.decrypt(
            CiphertextBlob=ciphertext,
            KeyId=master_key_identifier,
        )
        logger.debug("Unwrapped DEK with CMK %s", master_key_identifier)
        return response["Plaintext"]


class AwsKmsClientFactory:
    """Factory callable for ``CryptoFactory(kms_client_factory)``.

    Usage::

        factory = AwsKmsClientFactory(region="us-east-1")
        crypto_factory = CryptoFactory(factory)

    Or with RBAC (STS assume-role)::

        factory = AwsKmsClientFactory(
            region="us-east-1",
            role_arn="arn:aws:iam::123456789012:role/pme-fraud-analyst",
        )
    """

    def __init__(
        self,
        region: str = "us-east-1",
        role_arn: Optional[str] = None,
    ) -> None:
        self._region = region
        self._role_arn = role_arn

    def __call__(self, kms_connection_config: str) -> AwsKmsClient:
        """Create an AwsKmsClient, merging factory defaults with the
        per-call *kms_connection_config* (JSON) from PyArrow.

        PyArrow may pass an empty or partial config; we fill in region
        and role_arn from the factory defaults.
        """
        # Parse whatever PyArrow sends (may be empty string)
        try:
            conf = json.loads(kms_connection_config) if kms_connection_config else {}
        except (json.JSONDecodeError, TypeError):
            conf = {}

        # Apply factory defaults
        conf.setdefault(_CONF_KEY_REGION, self._region)
        if self._role_arn and _CONF_KEY_ROLE_ARN not in conf:
            conf[_CONF_KEY_ROLE_ARN] = self._role_arn

        return AwsKmsClient(json.dumps(conf))
