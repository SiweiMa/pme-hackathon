"""AWS Lambda handler for the Snowflake External Function decryption proxy.

Accepts Snowflake external-function POST requests via API Gateway (AWS_PROXY
integration), decrypts PME-encrypted Parquet from S3 using the ``pme/``
library, applies RBAC null-masking based on the role name passed as a
function argument, and returns JSON rows in Snowflake's expected format.

Snowflake wire format (request body):
    {"data": [[row_index, <user_arg_1>, <user_arg_2>, ...], ...]}

Snowflake wire format (response body):
    {"data": [[row_index, <json_result>], ...]}

Environment variables (set by Terraform):
    S3_BUCKET       - S3 bucket containing PME-encrypted data
    S3_PREFIX       - S3 key prefix (e.g. "pme-data")
    FOOTER_KEY_ARN  - KMS alias ARN for footer encryption
    PCI_KEY_ARN     - KMS alias ARN for PCI column encryption
    PII_KEY_ARN     - KMS alias ARN for PII column encryption
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import boto3
import pyarrow as pa

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
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

S3_KEY = f"{S3_PREFIX}/customer_data_encrypted.parquet"

# KMS key aliases
PCI_KEY_ALIAS = "pme-hackathon-pci-key"
PII_KEY_ALIAS = "pme-hackathon-pii-key"

# ---------------------------------------------------------------------------
# RBAC: role name → denied KMS key aliases
# ---------------------------------------------------------------------------

_ROLE_DENIED_KEYS: dict[str, frozenset[str]] = {
    "fraud-analyst": frozenset(),
    "marketing-analyst": frozenset({PCI_KEY_ALIAS}),
    "junior-analyst": frozenset({PCI_KEY_ALIAS, PII_KEY_ALIAS}),
}

_DEFAULT_DENIED_KEYS = frozenset({PCI_KEY_ALIAS, PII_KEY_ALIAS})


def _denied_keys_for_role(role_name: str) -> frozenset[str]:
    """Return the set of KMS key aliases to deny for the given role name."""
    denied = _ROLE_DENIED_KEYS.get(role_name)
    if denied is not None:
        return denied
    logger.warning("Unknown role %r — applying maximum restriction", role_name)
    return _DEFAULT_DENIED_KEYS


# ---------------------------------------------------------------------------
# Config builder (same pattern as connector/handler.py)
# ---------------------------------------------------------------------------


def _build_config() -> PmeConfig:
    """Build PmeConfig from environment variables."""
    return PmeConfig(
        footer_key=KmsKeyConfig(
            key_arn=FOOTER_KEY_ARN,
            alias="pme-hackathon-footer-key",
        ),
        column_groups=[
            ColumnGroupConfig(
                name="pci",
                kms_key=KmsKeyConfig(
                    key_arn=PCI_KEY_ARN,
                    alias=PCI_KEY_ALIAS,
                ),
                columns=["ssn"],
            ),
            ColumnGroupConfig(
                name="pii",
                kms_key=KmsKeyConfig(
                    key_arn=PII_KEY_ARN,
                    alias=PII_KEY_ALIAS,
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
# In-memory cache: avoids re-downloading / re-decrypting on warm invocations
# ---------------------------------------------------------------------------

_cache: dict[str, pa.Table] = {}
_cache_etag: Optional[str] = None


def _get_decrypted_table(denied_keys: frozenset[str]) -> pa.Table:
    """Return the decrypted + RBAC-masked table, using cache when possible."""
    global _cache_etag

    s3 = boto3.client("s3")

    # Check ETag for cache invalidation
    head = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
    etag = head["ETag"]

    cache_key = f"{etag}:{','.join(sorted(denied_keys))}"
    if cache_key in _cache:
        logger.info("Cache hit for ETag=%s role_denied=%s", etag, denied_keys)
        return _cache[cache_key]

    # Invalidate all entries if ETag changed (new file version)
    if etag != _cache_etag:
        _cache.clear()
        _cache_etag = etag
        logger.info("ETag changed to %s — cache cleared", etag)

    # Download and decrypt
    local_path = Path(tempfile.mktemp(suffix=".parquet", dir="/tmp"))
    logger.info("Downloading s3://%s/%s to %s", S3_BUCKET, S3_KEY, local_path)
    s3.download_file(S3_BUCKET, S3_KEY, str(local_path))

    try:
        config = _build_config()
        factory = AwsKmsClientFactory(
            region=config.region,
            alias_to_arn=config.alias_to_arn,
        )
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
    finally:
        if local_path.exists():
            local_path.unlink()

    _cache[cache_key] = table
    return table


# ---------------------------------------------------------------------------
# Row serialization
# ---------------------------------------------------------------------------


def _table_row_to_json(table: pa.Table, row_index: int) -> str:
    """Serialize a single row of the Arrow table to a JSON string."""
    if row_index < 0 or row_index >= table.num_rows:
        return json.dumps({"error": f"row_index {row_index} out of range (0..{table.num_rows - 1})"})

    row = {}
    for col_name in table.column_names:
        value = table.column(col_name)[row_index].as_py()
        row[col_name] = value
    return json.dumps(row)


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------


def lambda_handler(event, context):
    """Snowflake External Function entry point.

    Expects API Gateway proxy event with Snowflake's wire format in the body:
        {"data": [[row_index, data_row_index, role_name], ...]}

    Returns API Gateway proxy response with Snowflake's wire format:
        {"data": [[row_index, json_string], ...]}
    """
    try:
        body = event.get("body", "{}")
        if isinstance(body, str):
            body = json.loads(body)

        rows = body.get("data", [])
        if not rows:
            return _api_response(200, {"data": []})

        results = []
        for row in rows:
            row_index = row[0]
            # Snowflake passes: row_index, arg1 (data_row_index), arg2 (role_name)
            data_row_index = int(row[1]) if len(row) > 1 else 0
            role_name = str(row[2]) if len(row) > 2 else "junior-analyst"

            denied_keys = _denied_keys_for_role(role_name)

            try:
                table = _get_decrypted_table(denied_keys)
                json_row = _table_row_to_json(table, data_row_index)
            except Exception as exc:
                logger.exception("Error decrypting row %d", data_row_index)
                json_row = json.dumps({"error": str(exc)})

            results.append([row_index, json_row])

        return _api_response(200, {"data": results})

    except Exception as exc:
        logger.exception("Unhandled error in lambda_handler")
        # Snowflake expects 200 with error info, not 4xx/5xx
        return _api_response(200, {"data": [[0, json.dumps({"error": str(exc)})]]})


def _api_response(status_code: int, body: dict) -> dict:
    """Build an API Gateway proxy response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
