"""AWS Lambda handler for PME encryption pipeline.

Encrypts CSV data from S3 into column-level encrypted Parquet using
PyArrow Modular Encryption with AWS KMS keys.

Environment variables (set by Terraform):
    S3_BUCKET       — S3 bucket for input/output data
    S3_PREFIX       — S3 key prefix for encrypted Parquet output
    FOOTER_KEY_ARN  — KMS alias ARN for footer encryption
    PCI_KEY_ARN     — KMS alias ARN for PCI column encryption
    PII_KEY_ARN     — KMS alias ARN for PII column encryption
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os

import boto3
import pyarrow as pa

from pme.src.config import ColumnGroupConfig, KmsKeyConfig, PmeConfig
from pme.src.encryption import write_encrypted_to_s3
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

# Default input CSV location
DEFAULT_INPUT_KEY = "raw-data/Hackathon_customer_data.csv"
DEFAULT_OUTPUT_FILENAME = "customer_data_encrypted.parquet"


def _build_config(s3_prefix: str) -> PmeConfig:
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
        s3_prefix=s3_prefix,
        region=AWS_REGION,
        plaintext_footer=True,
    )


def _load_csv_from_s3(bucket: str, key: str) -> pa.Table:
    """Download a CSV from S3 and return a PyArrow Table."""
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")

    rows: list[dict] = []
    reader = csv.DictReader(io.StringIO(body))
    for row in reader:
        row["balance"] = float(row["balance"])
        rows.append(row)

    return pa.Table.from_pylist(rows)


def lambda_handler(event, context):
    """Encrypt CSV from S3 and write PME-encrypted Parquet back to S3.

    Event (all fields optional):
        input_s3_key     — S3 key of the input CSV (default: raw-data/Hackathon_customer_data.csv)
        output_filename  — Name for the output Parquet file (default: customer_data_encrypted.parquet)
        s3_prefix        — S3 prefix for output (default: from S3_PREFIX env var)
    """
    input_key = event.get("input_s3_key", DEFAULT_INPUT_KEY)
    output_filename = event.get("output_filename", DEFAULT_OUTPUT_FILENAME)
    s3_prefix = event.get("s3_prefix", S3_PREFIX)

    logger.info(
        "Starting PME encryption: bucket=%s input=%s output=%s/%s",
        S3_BUCKET, input_key, s3_prefix, output_filename,
    )

    # Load CSV from S3
    table = _load_csv_from_s3(S3_BUCKET, input_key)
    logger.info("Loaded %d rows, %d columns from s3://%s/%s",
                table.num_rows, table.num_columns, S3_BUCKET, input_key)

    # Build encryption config
    config = _build_config(s3_prefix)

    # Create KMS client factory (uses Lambda execution role via default boto3 chain)
    factory = AwsKmsClientFactory(
        region=config.region,
        alias_to_arn=config.alias_to_arn,
    )

    # Write encrypted Parquet to S3
    s3_uri = write_encrypted_to_s3(table, config, factory, filename=output_filename)
    logger.info("Wrote encrypted Parquet to %s", s3_uri)

    return {
        "statusCode": 200,
        "body": {
            "output_s3_uri": s3_uri,
            "rows_encrypted": table.num_rows,
            "columns_encrypted": config.all_encrypted_columns,
            "input_s3_key": input_key,
        },
    }
