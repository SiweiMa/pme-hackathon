"""Encrypted Parquet write pipeline using PyArrow Modular Encryption.

This module provides functions to write PyArrow Tables as PME-encrypted
Parquet files (locally or to S3) using column-level encryption with
AWS KMS-managed keys.

Encryption flow:
    1. PyArrow generates random DEK (256-bit AES) per column group
    2. DEK → KmsClient.wrap_key(DEK, kms_arn) → KMS encrypts DEK
    3. PyArrow encrypts column data locally with plaintext DEK (AES-GCM)
    4. Wrapped DEK stored in Parquet footer
    5. Plaintext DEK discarded (never persisted)
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path
from typing import Callable, Union

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

from pme.src.config import PmeConfig

logger = logging.getLogger(__name__)

# Magic bytes at start of Parquet files
PARQUET_MAGIC_PLAINTEXT = b"PAR1"  # standard / plaintext footer
PARQUET_MAGIC_ENCRYPTED = b"PARE"  # encrypted footer


def build_encryption_config(config: PmeConfig) -> pe.EncryptionConfiguration:
    """Build a PyArrow EncryptionConfiguration from PmeConfig.

    Uses key aliases (not full ARNs) because PyArrow's C++ layer uses
    colons as delimiters when parsing key identifiers. The KMS client
    receives these aliases and resolves them to full ARNs.
    """
    return pe.EncryptionConfiguration(
        footer_key=config.footer_key.alias,
        column_keys=config.kms_column_keys,
        encryption_algorithm=config.encryption_algorithm.value,
        plaintext_footer=config.plaintext_footer,
        double_wrapping=config.double_wrapping,
        cache_lifetime=timedelta(seconds=config.cache_lifetime_seconds),
        internal_key_material=True,
    )


def build_kms_connection_config(config: PmeConfig) -> pe.KmsConnectionConfig:
    """Build a PyArrow KmsConnectionConfig from PmeConfig."""
    kms_conn = pe.KmsConnectionConfig()
    kms_conn.custom_kms_conf = {"region": config.region}
    return kms_conn


def build_file_encryption_properties(
    config: PmeConfig,
    crypto_factory: pe.CryptoFactory,
    kms_conn_config: pe.KmsConnectionConfig,
) -> pq.FileEncryptionProperties:
    """Build FileEncryptionProperties from PmeConfig.

    Parameters
    ----------
    config : PmeConfig
        Encryption settings (keys, algorithm, footer mode).
    crypto_factory : pe.CryptoFactory
        CryptoFactory wrapping a KmsClient (real or in-memory).
    kms_conn_config : pe.KmsConnectionConfig
        KMS connection parameters (region, optional role).

    Returns
    -------
    pq.FileEncryptionProperties
        Ready to pass to ``pq.write_table(..., encryption_properties=...)``.
    """
    enc_config = build_encryption_config(config)
    return crypto_factory.file_encryption_properties(kms_conn_config, enc_config)


def write_encrypted_parquet(
    table: pa.Table,
    path: Union[str, Path],
    config: PmeConfig,
    kms_client_factory: Callable,
) -> Path:
    """Write a PME-encrypted Parquet file to a local path.

    Parameters
    ----------
    table : pa.Table
        Data to encrypt and write.
    path : str or Path
        Local file path for the output Parquet file.
    config : PmeConfig
        Encryption settings.
    kms_client_factory : callable
        Factory for KmsClient instances (e.g. AwsKmsClientFactory or
        InMemoryKmsClientFactory).

    Returns
    -------
    Path
        The path of the written file.
    """
    path = Path(path)
    crypto_factory = pe.CryptoFactory(kms_client_factory)
    kms_conn_config = build_kms_connection_config(config)
    file_enc_props = build_file_encryption_properties(
        config, crypto_factory, kms_conn_config
    )

    pq.write_table(table, str(path), encryption_properties=file_enc_props)
    logger.info("Wrote encrypted Parquet to %s (%d bytes)", path, path.stat().st_size)
    return path


def write_encrypted_to_s3(
    table: pa.Table,
    config: PmeConfig,
    kms_client_factory: Callable,
    filename: str = "data.parquet",
) -> str:
    """Write a PME-encrypted Parquet file to S3.

    Uses s3fs to open a file-like object and writes via ParquetWriter,
    since ``pq.write_to_dataset`` does not support encryption_properties.

    Parameters
    ----------
    table : pa.Table
        Data to encrypt and write.
    config : PmeConfig
        Encryption settings (includes S3 bucket/prefix).
    kms_client_factory : callable
        Factory for KmsClient instances.
    filename : str
        Name of the Parquet file within the S3 prefix.

    Returns
    -------
    str
        The full S3 URI of the written file.
    """
    import s3fs

    crypto_factory = pe.CryptoFactory(kms_client_factory)
    kms_conn_config = build_kms_connection_config(config)
    file_enc_props = build_file_encryption_properties(
        config, crypto_factory, kms_conn_config
    )

    s3_key = f"{config.s3_bucket}/{config.s3_prefix}/{filename}"
    s3_uri = f"s3://{s3_key}"

    fs = s3fs.S3FileSystem()
    with fs.open(s3_key, "wb") as f:
        writer = pq.ParquetWriter(f, table.schema, encryption_properties=file_enc_props)
        writer.write_table(table)
        writer.close()

    logger.info("Wrote encrypted Parquet to %s", s3_uri)
    return s3_uri


def verify_pme_file(path: Union[str, Path], expect_encrypted_footer: bool) -> bool:
    """Verify Parquet file magic bytes match the expected footer mode.

    Parameters
    ----------
    path : str or Path
        Path to the Parquet file.
    expect_encrypted_footer : bool
        If True, expect PARE (encrypted footer).
        If False, expect PAR1 (plaintext footer, columns still encrypted).

    Returns
    -------
    bool
        True if magic bytes match expectation.

    Raises
    ------
    ValueError
        If the file is too small or magic bytes are unrecognized.
    """
    path = Path(path)
    data = path.read_bytes()

    if len(data) < 4:
        raise ValueError(f"File too small ({len(data)} bytes): {path}")

    magic = data[:4]
    expected = PARQUET_MAGIC_ENCRYPTED if expect_encrypted_footer else PARQUET_MAGIC_PLAINTEXT

    if magic not in (PARQUET_MAGIC_PLAINTEXT, PARQUET_MAGIC_ENCRYPTED):
        raise ValueError(f"Unrecognized magic bytes {magic!r} in {path}")

    match = magic == expected
    logger.debug(
        "Magic bytes check: got %r, expected %r → %s",
        magic, expected, "PASS" if match else "FAIL",
    )
    return match
