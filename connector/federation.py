"""Athena Federation wire protocol serialization.

Implements the JSON + Base64-encoded Arrow IPC format expected by Athena's
federated query engine.  The Java SDK uses ``BlockSerializer`` and
``SchemaSerDeV3`` to encode/decode Arrow data — this module replicates that
format using PyArrow's IPC writer.

Wire format for a record block (``serialize_block``):
    [4-byte big-endian schema size][schema IPC message][record batch IPC message]

All binary payloads are Base64-encoded in JSON responses.

References:
    https://github.com/awslabs/aws-athena-query-federation
"""

from __future__ import annotations

import base64
import io
import struct

import pyarrow as pa
import pyarrow.ipc as ipc


# ---------------------------------------------------------------------------
# Low-level serialization helpers
# ---------------------------------------------------------------------------


def serialize_schema(schema: pa.Schema) -> str:
    """Serialize a PyArrow schema to Base64-encoded Arrow IPC bytes.

    Matches the Java SDK's ``SchemaSerDeV3.serialize(Schema)``.

    Parameters
    ----------
    schema : pa.Schema
        Arrow schema to serialize.

    Returns
    -------
    str
        Base64-encoded schema IPC message.
    """
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.close()
    buf = sink.getvalue()
    return base64.b64encode(buf.to_pybytes()).decode("ascii")


def serialize_block(table: pa.Table) -> str:
    """Serialize a PyArrow Table to the Athena Federation block format.

    The block format is::

        [4-byte big-endian: schema IPC size]
        [schema IPC stream bytes]
        [record batch IPC stream bytes]

    This matches the Java SDK's ``BlockSerializer.serialize(Block)`` layout.

    Parameters
    ----------
    table : pa.Table
        The Arrow table to serialize as a single block.

    Returns
    -------
    str
        Base64-encoded block bytes.
    """
    # Serialize schema as an IPC stream (schema message only, no batches)
    schema_sink = pa.BufferOutputStream()
    schema_writer = ipc.new_stream(schema_sink, table.schema)
    schema_writer.close()
    schema_bytes = schema_sink.getvalue().to_pybytes()

    # Serialize record batch as an IPC stream
    batch_sink = pa.BufferOutputStream()
    batch_writer = ipc.new_stream(batch_sink, table.schema)
    # Combine all chunks into a single batch for the block
    batch = table.combine_chunks().to_batches()[0]
    batch_writer.write_batch(batch)
    batch_writer.close()
    batch_bytes = batch_sink.getvalue().to_pybytes()

    # Combine: [4-byte schema size][schema IPC][batch IPC]
    buf = io.BytesIO()
    buf.write(struct.pack(">I", len(schema_bytes)))
    buf.write(schema_bytes)
    buf.write(batch_bytes)

    return base64.b64encode(buf.getvalue()).decode("ascii")


# ---------------------------------------------------------------------------
# Response builders — one per Athena Federation response type
# ---------------------------------------------------------------------------


def ping_response(catalog_name: str, query_id: str, source_type: str) -> dict:
    """Build a PingResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    query_id : str
        The query ID from the request.
    source_type : str
        Connector source type identifier.

    Returns
    -------
    dict
        JSON-serializable PingResponse.
    """
    return {
        "@type": "PingResponse",
        "catalogName": catalog_name,
        "queryId": query_id,
        "sourceType": source_type,
        "capabilities": 23,
    }


def list_schemas_response(
    catalog_name: str, request_type: str, schemas: list[str],
) -> dict:
    """Build a ListSchemasResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    request_type : str
        The ``@type`` from the original request.
    schemas : list of str
        Schema (database) names.

    Returns
    -------
    dict
        JSON-serializable ListSchemasResponse.
    """
    return {
        "@type": "ListSchemasResponse",
        "catalogName": catalog_name,
        "schemas": schemas,
        "requestType": request_type,
    }


def list_tables_response(
    catalog_name: str,
    request_type: str,
    tables: list[dict],
) -> dict:
    """Build a ListTablesResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    request_type : str
        The ``@type`` from the original request.
    tables : list of dict
        Each dict has ``schemaName`` and ``tableName``.

    Returns
    -------
    dict
        JSON-serializable ListTablesResponse.
    """
    return {
        "@type": "ListTablesResponse",
        "catalogName": catalog_name,
        "tables": tables,
        "requestType": request_type,
    }


def get_table_response(
    catalog_name: str,
    request_type: str,
    table_name: dict,
    schema: pa.Schema,
) -> dict:
    """Build a GetTableResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    request_type : str
        The ``@type`` from the original request.
    table_name : dict
        ``{"schemaName": ..., "tableName": ...}``.
    schema : pa.Schema
        The Arrow schema of the table.

    Returns
    -------
    dict
        JSON-serializable GetTableResponse.
    """
    return {
        "@type": "GetTableResponse",
        "catalogName": catalog_name,
        "tableName": table_name,
        "schema": {"schema": serialize_schema(schema)},
        "requestType": request_type,
    }


def get_table_layout_response(
    catalog_name: str, request_type: str, partitions: pa.Table,
) -> dict:
    """Build a GetTableLayoutResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    request_type : str
        The ``@type`` from the original request.
    partitions : pa.Table
        Single-row table representing partition information.

    Returns
    -------
    dict
        JSON-serializable GetTableLayoutResponse.
    """
    return {
        "@type": "GetTableLayoutResponse",
        "catalogName": catalog_name,
        "partitions": {"aId": serialize_block(partitions)},
        "requestType": request_type,
    }


def get_splits_response(
    catalog_name: str,
    request_type: str,
    splits: list[dict],
    continuation_token: str | None = None,
) -> dict:
    """Build a GetSplitsResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    request_type : str
        The ``@type`` from the original request.
    splits : list of dict
        Each split has ``catalogName``, ``schemaName``, ``tableName``,
        ``spillLocation``, and ``properties``.
    continuation_token : str or None
        If set, Athena will call back for more splits.

    Returns
    -------
    dict
        JSON-serializable GetSplitsResponse.
    """
    resp: dict = {
        "@type": "GetSplitsResponse",
        "catalogName": catalog_name,
        "splits": splits,
        "requestType": request_type,
    }
    if continuation_token is not None:
        resp["continuationToken"] = continuation_token
    return resp


def read_records_response(
    catalog_name: str,
    request_type: str,
    records: pa.Table,
) -> dict:
    """Build a ReadRecordsResponse.

    Parameters
    ----------
    catalog_name : str
        The data catalog name.
    request_type : str
        The ``@type`` from the original request.
    records : pa.Table
        Decrypted (and RBAC-masked) Arrow table.

    Returns
    -------
    dict
        JSON-serializable ReadRecordsResponse.
    """
    return {
        "@type": "ReadRecordsResponse",
        "catalogName": catalog_name,
        "records": {"aId": serialize_block(records)},
        "requestType": request_type,
    }
