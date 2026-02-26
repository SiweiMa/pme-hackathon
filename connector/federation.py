"""Athena Federation wire protocol serialization.

Implements the JSON + Base64-encoded Arrow IPC format expected by Athena's
federated query engine.  The Java SDK uses ``BlockSerializer`` and
``SchemaSerDeV3`` to encode/decode Arrow data — this module replicates that
format using PyArrow's IPC writer.

Wire format for a record block (``serialize_block``):
    [4-byte big-endian schema size][schema IPC message][record batch IPC message]

All binary payloads are Base64-encoded in JSON responses.

The Java SDK uses Jackson ``@JsonTypeInfo`` with simple class names for
``@type`` and ``FederationType`` enum names for ``requestType``:
    PING, LIST_SCHEMAS, LIST_TABLES, GET_TABLE, GET_TABLE_LAYOUT,
    GET_SPLITS, READ_RECORDS

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

    Matches the Java SDK's ``SchemaSerDeV3.serialize(Schema)``:
    opens an ArrowStreamWriter, calls start() (writes schema message),
    then close() (writes EOS marker).

    Parameters
    ----------
    schema : pa.Schema
        Arrow schema to serialize.

    Returns
    -------
    str
        Base64-encoded schema IPC stream bytes.
    """
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.close()
    buf = sink.getvalue()
    return base64.b64encode(buf.to_pybytes()).decode("ascii")


def serialize_block(table: pa.Table) -> str:
    """Serialize a PyArrow Table to the Athena Federation block format.

    The block format is::

        [4-byte big-endian: schema IPC stream size]
        [schema IPC stream bytes (schema msg + EOS)]
        [record batch IPC stream bytes (schema msg + batch msg + EOS)]

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
#
# Field formats match the Java SDK's Jackson serialization:
#   - requestType: FederationType enum name (e.g. "LIST_SCHEMAS")
#   - schema: Base64 string (via SchemaSerDe.Serializer → writeBinary)
#   - records/partitions: Base64 string (via BlockSerDe.Serializer → writeBinary)
# ---------------------------------------------------------------------------


def ping_response(catalog_name: str, query_id: str, source_type: str) -> dict:
    """Build a PingResponse."""
    return {
        "@type": "PingResponse",
        "catalogName": catalog_name,
        "queryId": query_id,
        "sourceType": source_type,
        "capabilities": 23,
        "requestType": "PING",
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
        "schema": serialize_schema(schema),
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
        "partitions": serialize_block(partitions),
        "requestType": "GET_TABLE_LAYOUT",
    }


def get_splits_response(
    catalog_name: str,
    splits: list[dict],
    continuation_token: str | None = None,
) -> dict:
    """Build a GetSplitsResponse."""
    resp: dict = {
        "@type": "GetSplitsResponse",
        "catalogName": catalog_name,
        "splits": splits,
        "requestType": "GET_SPLITS",
    }
    if continuation_token is not None:
        resp["continuationToken"] = continuation_token
    return resp


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
        "schema": serialize_schema(schema),
        "records": serialize_block(records),
        "requestType": "READ_RECORDS",
    }
