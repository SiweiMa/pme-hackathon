"""Athena Federation wire protocol serialization.

Implements the JSON + Base64-encoded Arrow format expected by Athena's
federated query engine.  Based on the dacort/athena-federation-python-sdk
reference implementation.

Serialization uses ``pa.Schema.serialize().slice(4)`` and
``pa.RecordBatch.serialize().slice(4)`` to produce raw Arrow flatbuffer
bytes (minus the 4-byte continuation/padding prefix), then Base64-encodes
them.  This matches the Java SDK's ``SchemaSerDeV3`` and ``BlockSerializer``.

Block (records/partitions) format in JSON::

    {
        "aId": "<uuid>",
        "schema": "<base64 schema flatbuffer>",
        "records": "<base64 batch flatbuffer>"
    }

References:
    https://github.com/dacort/athena-federation-python-sdk
    https://github.com/awslabs/aws-athena-query-federation
"""

from __future__ import annotations

import base64
from uuid import uuid4

import pyarrow as pa


# ---------------------------------------------------------------------------
# Low-level serialization helpers
# ---------------------------------------------------------------------------


def encode_pyarrow_object(obj: pa.Schema | pa.RecordBatch) -> str:
    """Encode a PyArrow Schema or RecordBatch to Base64.

    Matches the dacort SDK's ``AthenaSDKUtils.encode_pyarrow_object``:
    calls ``.serialize()`` then slices off the first 4 bytes (continuation
    marker) before Base64-encoding.

    Parameters
    ----------
    obj : pa.Schema or pa.RecordBatch
        Arrow object to serialize.

    Returns
    -------
    str
        Base64-encoded flatbuffer bytes (without 4-byte prefix).
    """
    return base64.b64encode(obj.serialize().slice(4)).decode("utf-8")


def encode_block(table: pa.Table) -> dict:
    """Encode a PyArrow Table as an Athena Federation block dict.

    Parameters
    ----------
    table : pa.Table
        Arrow table to encode (combined into a single batch).

    Returns
    -------
    dict
        Block dict with ``aId``, ``schema``, and ``records`` keys.
    """
    batch = table.combine_chunks().to_batches()[0]
    return {
        "aId": str(uuid4()),
        "schema": encode_pyarrow_object(batch.schema),
        "records": encode_pyarrow_object(batch),
    }


# ---------------------------------------------------------------------------
# Response builders — one per Athena Federation response type
#
# Field formats match the dacort Python SDK / Java SDK Jackson serialization.
# ---------------------------------------------------------------------------


def ping_response(catalog_name: str, query_id: str, source_type: str) -> dict:
    """Build a PingResponse."""
    return {
        "@type": "PingResponse",
        "catalogName": catalog_name,
        "queryId": query_id,
        "sourceType": source_type,
        "capabilities": 23,
    }


def list_schemas_response(catalog_name: str, schemas: list[str]) -> dict:
    """Build a ListSchemasResponse."""
    return {
        "@type": "ListSchemasResponse",
        "catalogName": catalog_name,
        "schemas": schemas,
        "requestType": "LIST_SCHEMAS",
    }


def list_tables_response(catalog_name: str, tables: list[dict]) -> dict:
    """Build a ListTablesResponse."""
    return {
        "@type": "ListTablesResponse",
        "catalogName": catalog_name,
        "tables": tables,
        "requestType": "LIST_TABLES",
    }


def get_table_response(
    catalog_name: str,
    table_name: dict,
    schema: pa.Schema,
    partition_columns: list[str] | None = None,
) -> dict:
    """Build a GetTableResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    table_name : dict
        ``{"schemaName": ..., "tableName": ...}``.
    schema : pa.Schema
        The Arrow schema of the table.
    partition_columns : list of str or None
        Partition column names (empty for non-partitioned tables).
    """
    return {
        "@type": "GetTableResponse",
        "catalogName": catalog_name,
        "tableName": table_name,
        "schema": {"schema": encode_pyarrow_object(schema)},
        "partitionColumns": partition_columns or [],
        "requestType": "GET_TABLE",
    }


def get_table_layout_response(
    catalog_name: str,
    table_name: dict,
    partitions: pa.Table,
) -> dict:
    """Build a GetTableLayoutResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    table_name : dict
        ``{"schemaName": ..., "tableName": ...}``.
    partitions : pa.Table
        Single-row table representing partition information.
    """
    return {
        "@type": "GetTableLayoutResponse",
        "catalogName": catalog_name,
        "tableName": table_name,
        "partitions": encode_block(partitions),
        "requestType": "GET_TABLE_LAYOUT",
    }


def get_splits_response(
    catalog_name: str,
    splits: list[dict],
    continuation_token: str | None = None,
) -> dict:
    """Build a GetSplitsResponse."""
    return {
        "@type": "GetSplitsResponse",
        "catalogName": catalog_name,
        "splits": splits,
        "continuationToken": continuation_token,
        "requestType": "GET_SPLITS",
    }


def read_records_response(
    catalog_name: str,
    schema: pa.Schema,
    records: pa.Table,
) -> dict:
    """Build a ReadRecordsResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    schema : pa.Schema
        The Arrow schema (required by Java deserializer).
    records : pa.Table
        Decrypted (and RBAC-masked) Arrow table.
    """
    return {
        "@type": "ReadRecordsResponse",
        "catalogName": catalog_name,
        "schema": {"schema": encode_pyarrow_object(schema)},
        "records": encode_block(records),
        "requestType": "READ_RECORDS",
    }
