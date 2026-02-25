"""Encrypted Parquet read pipeline using PyArrow Modular Encryption.

This module provides functions to read PME-encrypted Parquet files
(locally or from S3) using column-level decryption with AWS KMS-managed
keys.

Decryption flow:
    1. PyArrow reads wrapped DEK from Parquet footer
    2. DEK → KmsClient.unwrap_key(wrapped_DEK, kms_alias) → KMS decrypts DEK
    3. PyArrow decrypts column data locally with plaintext DEK (AES-GCM)
    4. Table returned fully decrypted

RBAC model — partial access with null masking:
    ``read_with_partial_access`` reads the file with a full-access KMS
    client and then nulls out columns whose key aliases are in the
    caller's ``denied_key_aliases`` set.  This provides column-level
    access control at the application layer: users see the data they
    are allowed to see, and denied columns come back as null.

    PyArrow's C++ reader decrypts ALL column data (even with
    ``columns=``), so partial decryption at the crypto layer is not
    possible.  Application-level masking after a full-access read is
    the standard pattern for data platforms.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path
from typing import Callable, Optional, Sequence, Union

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

from pme.src.config import PmeConfig
from pme.src.encryption import build_kms_connection_config

logger = logging.getLogger(__name__)


def build_decryption_config(config: PmeConfig) -> pe.DecryptionConfiguration:
    """Build a PyArrow DecryptionConfiguration from PmeConfig.

    Parameters
    ----------
    config : PmeConfig
        Encryption/decryption settings (uses ``cache_lifetime_seconds``).

    Returns
    -------
    pe.DecryptionConfiguration
    """
    return pe.DecryptionConfiguration(
        cache_lifetime=timedelta(seconds=config.cache_lifetime_seconds),
    )


def build_file_decryption_properties(
    config: PmeConfig,
    crypto_factory: pe.CryptoFactory,
    kms_conn_config: pe.KmsConnectionConfig,
) -> pq.FileDecryptionProperties:
    """Build FileDecryptionProperties from PmeConfig.

    Parameters
    ----------
    config : PmeConfig
        Encryption/decryption settings.
    crypto_factory : pe.CryptoFactory
        CryptoFactory wrapping a KmsClient (real or in-memory).
    kms_conn_config : pe.KmsConnectionConfig
        KMS connection parameters (region, optional role).

    Returns
    -------
    pq.FileDecryptionProperties
        Ready to pass to ``pq.read_table(..., decryption_properties=...)``.
    """
    dec_config = build_decryption_config(config)
    return crypto_factory.file_decryption_properties(kms_conn_config, dec_config)


def read_encrypted_parquet(
    path: Union[str, Path],
    config: PmeConfig,
    kms_client_factory: Callable,
    *,
    columns: Optional[Sequence[str]] = None,
) -> pa.Table:
    """Read a PME-encrypted Parquet file from a local path.

    Parameters
    ----------
    path : str or Path
        Local file path of the encrypted Parquet file.
    config : PmeConfig
        Encryption/decryption settings.
    kms_client_factory : callable
        Factory for KmsClient instances (e.g. AwsKmsClientFactory or
        InMemoryKmsClientFactory).
    columns : sequence of str, optional
        Column names to include in the result.  Note: all column keys
        must still be accessible — PyArrow decrypts all column metadata
        regardless of this filter.

    Returns
    -------
    pa.Table
        The decrypted table.

    Raises
    ------
    PermissionError
        If the KMS client denies unwrap_key for any encrypted column's
        key (simulates KMS AccessDeniedException).
    """
    path = Path(path)
    crypto_factory = pe.CryptoFactory(kms_client_factory)
    kms_conn = build_kms_connection_config(config)
    fdp = build_file_decryption_properties(config, crypto_factory, kms_conn)

    table = pq.read_table(str(path), columns=columns, decryption_properties=fdp)
    logger.info(
        "Read encrypted Parquet from %s (%d rows, %d columns)",
        path,
        table.num_rows,
        table.num_columns,
    )
    return table


def read_with_partial_access(
    path: Union[str, Path],
    config: PmeConfig,
    kms_client_factory: Callable,
    *,
    denied_key_aliases: frozenset[str] = frozenset(),
    columns: Optional[Sequence[str]] = None,
) -> pa.Table:
    """Read a PME-encrypted Parquet file with column-level access control.

    Reads the full file using ``kms_client_factory`` (which must have
    access to **all** KMS keys), then replaces every column whose
    encryption key is in ``denied_key_aliases`` with nulls.

    This implements application-level RBAC: the service decrypts the
    file with full access, and the column masking enforces what each
    role is allowed to see.

    Parameters
    ----------
    path : str or Path
        Local file path of the encrypted Parquet file.
    config : PmeConfig
        Encryption/decryption settings.
    kms_client_factory : callable
        Full-access KMS client factory (must be able to decrypt all
        column groups).
    denied_key_aliases : frozenset of str
        KMS key aliases whose columns should be returned as null.
        Columns encrypted with these keys will contain only nulls.
    columns : sequence of str, optional
        Column names to include in the result.

    Returns
    -------
    pa.Table
        The table with denied columns nulled out.
    """
    # --- Step 1: full-access read -----------------------------------------
    table = read_encrypted_parquet(
        path, config, kms_client_factory, columns=columns,
    )

    if not denied_key_aliases:
        return table

    # --- Step 2: determine which columns to null --------------------------
    denied_columns: set[str] = set()
    for group in config.column_groups:
        if group.kms_key.alias in denied_key_aliases:
            denied_columns.update(group.columns)

    columns_to_null = [c for c in table.column_names if c in denied_columns]

    if not columns_to_null:
        return table

    # --- Step 3: replace denied columns with nulls ------------------------
    for col_name in columns_to_null:
        idx = table.column_names.index(col_name)
        col_type = table.schema.field(col_name).type
        null_array = pa.nulls(table.num_rows, type=col_type)
        table = table.set_column(idx, col_name, null_array)

    logger.info(
        "Partial-access read from %s (%d rows, %d columns, %d nulled)",
        path,
        table.num_rows,
        table.num_columns,
        len(columns_to_null),
    )
    return table


def read_encrypted_from_s3(
    config: PmeConfig,
    kms_client_factory: Callable,
    filename: str = "data.parquet",
    *,
    columns: Optional[Sequence[str]] = None,
) -> pa.Table:
    """Read a PME-encrypted Parquet file from S3.

    Parameters
    ----------
    config : PmeConfig
        Encryption/decryption settings (includes S3 bucket/prefix).
    kms_client_factory : callable
        Factory for KmsClient instances.
    filename : str
        Name of the Parquet file within the S3 prefix.
    columns : sequence of str, optional
        Column names to include in the result.

    Returns
    -------
    pa.Table
        The decrypted table.
    """
    import s3fs

    crypto_factory = pe.CryptoFactory(kms_client_factory)
    kms_conn = build_kms_connection_config(config)
    fdp = build_file_decryption_properties(config, crypto_factory, kms_conn)

    s3_key = f"{config.s3_bucket}/{config.s3_prefix}/{filename}"
    s3_uri = f"s3://{s3_key}"

    fs = s3fs.S3FileSystem()
    with fs.open(s3_key, "rb") as f:
        table = pq.read_table(f, columns=columns, decryption_properties=fdp)

    logger.info(
        "Read encrypted Parquet from %s (%d rows, %d columns)",
        s3_uri,
        table.num_rows,
        table.num_columns,
    )
    return table


def get_file_metadata(
    path: Union[str, Path],
    config: PmeConfig,
    kms_client_factory: Callable,
) -> pq.FileMetaData:
    """Read Parquet file metadata (schema, row count) from an encrypted file.

    Parameters
    ----------
    path : str or Path
        Local file path of the encrypted Parquet file.
    config : PmeConfig
        Encryption/decryption settings.
    kms_client_factory : callable
        Factory for KmsClient instances.

    Returns
    -------
    pq.FileMetaData
        Parquet file metadata including schema, row groups, and row count.
    """
    path = Path(path)
    crypto_factory = pe.CryptoFactory(kms_client_factory)
    kms_conn = build_kms_connection_config(config)
    fdp = build_file_decryption_properties(config, crypto_factory, kms_conn)

    pf = pq.ParquetFile(str(path), decryption_properties=fdp)
    return pf.metadata
