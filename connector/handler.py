"""AWS Lambda handler for the Athena Federated Connector (decryption proxy).

Implements the Athena Federation JSON+Arrow wire protocol.  Routes requests
by ``@type`` field, decrypts PME-encrypted Parquet from S3 using the ``pme/``
library, applies RBAC null-masking based on the caller's IAM identity, and
returns Arrow IPC data to Athena.

Environment variables (set by Terraform):
    S3_BUCKET       - S3 bucket containing PME-encrypted data
    S3_PREFIX       - S3 key prefix (e.g. "pme-data")
    FOOTER_KEY_ARN  - KMS alias ARN for footer encryption
    PCI_KEY_ARN     - KMS alias ARN for PCI column encryption
    PII_KEY_ARN     - KMS alias ARN for PII column encryption
    SPILL_BUCKET    - S3 bucket for Athena spill data
    SPILL_PREFIX    - S3 key prefix for spill data
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

import boto3
import pyarrow as pa

from connector.federation import (
    get_splits_response,
    get_table_layout_response,
    get_table_response,
    list_schemas_response,
    list_tables_response,
    ping_response,
    read_records_response,
)
from connector.rbac import denied_keys_for_caller
from pme.src.config import ColumnGroupConfig, KmsKeyConfig, PmeConfig
from pme.src.decryption import read_with_partial_access
from pme.src.kms_client import AwsKmsClientFactory

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration from environment variables
# ---------------------------------------------------------------------------

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "pme-data")
FOOTER_KEY_ARN = os.environ.get("FOOTER_KEY_ARN", "")
PCI_KEY_ARN = os.environ.get("PCI_KEY_ARN", "")
PII_KEY_ARN = os.environ.get("PII_KEY_ARN", "")
SPILL_BUCKET = os.environ.get("SPILL_BUCKET", "")
SPILL_PREFIX = os.environ.get("SPILL_PREFIX", "athena-spill")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

# Schema and table identifiers
CATALOG_NAME = "pwe-hackathon-pme-connector"
SCHEMA_NAME = "pwe-hackathon-pme-db"
TABLE_NAME = "customer_data"
SOURCE_TYPE = "pme_connector"

# Arrow schema for the customer_data table
TABLE_SCHEMA = pa.schema([
    pa.field("first_name", pa.string()),
    pa.field("last_name", pa.string()),
    pa.field("ssn", pa.string()),
    pa.field("email", pa.string()),
    pa.field("xid", pa.string()),
    pa.field("balance", pa.float64()),
])

TABLE_NAME_OBJ = {"schemaName": SCHEMA_NAME, "tableName": TABLE_NAME}


# ---------------------------------------------------------------------------
# Config builder (same pattern as lambda/handler.py)
# ---------------------------------------------------------------------------


def _build_config() -> PmeConfig:
    """Build PmeConfig from environment variables."""
    return PmeConfig(
        footer_key=KmsKeyConfig(
            key_arn=FOOTER_KEY_ARN,
            alias="pwe-hackathon-footer-key",
        ),
        column_groups=[
            ColumnGroupConfig(
                name="pci",
                kms_key=KmsKeyConfig(
                    key_arn=PCI_KEY_ARN,
                    alias="pwe-hackathon-pci-key",
                ),
                columns=["ssn"],
            ),
            ColumnGroupConfig(
                name="pii",
                kms_key=KmsKeyConfig(
                    key_arn=PII_KEY_ARN,
                    alias="pwe-hackathon-pii-key",
                ),
                columns=["first_name", "last_name", "email"],
            ),
        ],
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        region=AWS_REGION,
        plaintext_footer=True,
    )


# ---------------------------------------------------------------------------
# S3 download helper
# ---------------------------------------------------------------------------


def _download_pme_file(s3_key: str) -> Path:
    """Download PME Parquet file from S3 to Lambda ephemeral storage."""
    s3 = boto3.client("s3")
    local_path = Path(tempfile.mktemp(suffix=".parquet", dir="/tmp"))
    logger.info("Downloading s3://%s/%s to %s", S3_BUCKET, s3_key, local_path)
    s3.download_file(S3_BUCKET, s3_key, str(local_path))
    logger.info("Downloaded %d bytes", local_path.stat().st_size)
    return local_path


# ---------------------------------------------------------------------------
# Request handlers
# ---------------------------------------------------------------------------


def _extract_caller_arn(event: dict) -> str:
    """Extract the caller's IAM ARN from the federation request."""
    identity = event.get("identity", {})
    return identity.get("arn", "")


def _handle_ping(event: dict) -> dict:
    query_id = event.get("queryId", "")
    catalog = event.get("catalogName", CATALOG_NAME)
    return ping_response(catalog, query_id, SOURCE_TYPE)


def _handle_list_schemas(event: dict) -> dict:
    catalog = event.get("catalogName", CATALOG_NAME)
    return list_schemas_response(catalog, [SCHEMA_NAME])


def _handle_list_tables(event: dict) -> dict:
    catalog = event.get("catalogName", CATALOG_NAME)
    return list_tables_response(catalog, [TABLE_NAME_OBJ])


def _handle_get_table(event: dict) -> dict:
    catalog = event.get("catalogName", CATALOG_NAME)
    return get_table_response(catalog, TABLE_NAME_OBJ, TABLE_SCHEMA)


def _handle_get_table_layout(event: dict) -> dict:
    catalog = event.get("catalogName", CATALOG_NAME)
    # Single partition — no partitioning for 100 rows
    partition_schema = pa.schema([pa.field("partition_id", pa.int32())])
    partitions = pa.table({"partition_id": [0]}, schema=partition_schema)
    return get_table_layout_response(catalog, TABLE_NAME_OBJ, partitions)


def _handle_get_splits(event: dict) -> dict:
    catalog = event.get("catalogName", CATALOG_NAME)
    split = {
        "spillLocation": {
            "@type": "S3SpillLocation",
            "bucket": SPILL_BUCKET,
            "key": SPILL_PREFIX,
            "directory": True,
        },
        "encryptionKey": {
            "key": "",
            "nonce": "",
        },
        "properties": {
            "s3_key": f"{S3_PREFIX}/customer_data_encrypted.parquet",
        },
    }
    return get_splits_response(catalog, [split])


def _handle_read_records(event: dict) -> dict:
    """Core handler: decrypt PME data, apply RBAC, return Arrow block."""
    catalog = event.get("catalogName", CATALOG_NAME)

    # 1. Determine caller identity and RBAC restrictions
    caller_arn = _extract_caller_arn(event)
    denied_keys = denied_keys_for_caller(caller_arn)
    logger.info(
        "ReadRecords: caller=%s denied_keys=%s",
        caller_arn,
        denied_keys or "(none — full access)",
    )

    # 2. Determine which file to read from the split properties
    split = event.get("split", {})
    properties = split.get("properties", {})
    s3_key = properties.get(
        "s3_key", f"{S3_PREFIX}/customer_data_encrypted.parquet",
    )

    # 3. Download PME file from S3 to /tmp
    local_path = _download_pme_file(s3_key)

    try:
        # 4. Build config and KMS factory
        config = _build_config()
        factory = AwsKmsClientFactory(
            region=config.region,
            alias_to_arn=config.alias_to_arn,
        )

        # 5. Decrypt with RBAC null-masking
        table = read_with_partial_access(
            local_path,
            config,
            factory,
            denied_key_aliases=denied_keys,
        )

        logger.info(
            "Decrypted %d rows, %d columns (denied %d key groups)",
            table.num_rows,
            table.num_columns,
            len(denied_keys),
        )

        # 6. Serialize to Arrow block and return
        return read_records_response(catalog, TABLE_SCHEMA, table)

    finally:
        # Clean up temp file
        if local_path.exists():
            local_path.unlink()


# ---------------------------------------------------------------------------
# Request routing
# ---------------------------------------------------------------------------

_HANDLERS = {
    "PingRequest": _handle_ping,
    "ListSchemasRequest": _handle_list_schemas,
    "ListTablesRequest": _handle_list_tables,
    "GetTableRequest": _handle_get_table,
    "GetTableLayoutRequest": _handle_get_table_layout,
    "GetSplitsRequest": _handle_get_splits,
    "ReadRecordsRequest": _handle_read_records,
}


def lambda_handler(event, context):
    """Athena Federation connector entry point.

    Routes federation requests by the ``@type`` field in the event payload
    to the appropriate handler function.
    """
    request_type = event.get("@type", "")
    logger.info("Federation request: %s | event keys: %s", request_type, list(event.keys()))
    # Log full event for debugging (excluding large binary fields)
    debug_event = {k: v for k, v in event.items() if k not in ("schema",)}
    logger.info("Request payload: %s", json.dumps(debug_event, default=str)[:2000])

    handler = _HANDLERS.get(request_type)
    if handler is None:
        logger.error("Unknown request type: %s", request_type)
        return {
            "statusCode": 400,
            "body": f"Unknown request type: {request_type}",
        }

    response = handler(event)
    # Log response (truncate large base64 fields)
    debug_resp = {}
    for k, v in response.items():
        if isinstance(v, str) and len(v) > 200:
            debug_resp[k] = v[:100] + "...<truncated>"
        else:
            debug_resp[k] = v
    logger.info("Response: %s", json.dumps(debug_resp, default=str)[:2000])
    return response
