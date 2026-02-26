#!/usr/bin/env bash
# register_iceberg_glue.sh — Find Iceberg metadata in S3 and register in Glue
#
# After Snowflake writes data to the Iceberg table, this script discovers the
# latest metadata.json and updates the Glue catalog so Athena can query it.
#
# Usage: ./register_iceberg_glue.sh
set -euo pipefail

BUCKET="pme-hackathon-iceberg-651767347247"
PREFIX="iceberg/auth_data/metadata/"
REGION="us-east-2"
GLUE_DB="pme-hackathon-iceberg-db"
GLUE_TABLE="auth_data"

echo "Searching for Iceberg metadata in s3://${BUCKET}/${PREFIX} ..."

METADATA_FILE=$(aws s3api list-objects-v2 \
  --bucket "$BUCKET" \
  --prefix "$PREFIX" \
  --query 'sort_by(Contents, &LastModified)[-1].Key' \
  --output text \
  --region "$REGION")

if [ -z "$METADATA_FILE" ] || [ "$METADATA_FILE" = "None" ]; then
  echo "ERROR: No Iceberg metadata found in s3://${BUCKET}/${PREFIX}"
  echo "Make sure the Snowflake COPY INTO has completed first."
  exit 1
fi

METADATA_LOCATION="s3://${BUCKET}/${METADATA_FILE}"
echo "Latest Iceberg metadata: ${METADATA_LOCATION}"

echo "Updating Glue table ${GLUE_DB}.${GLUE_TABLE} ..."

aws glue update-table \
  --region "$REGION" \
  --database-name "$GLUE_DB" \
  --table-input "$(cat <<EOF
{
  "Name": "${GLUE_TABLE}",
  "TableType": "EXTERNAL_TABLE",
  "Parameters": {
    "table_type": "ICEBERG",
    "metadata_location": "${METADATA_LOCATION}"
  },
  "StorageDescriptor": {
    "Location": "s3://${BUCKET}/iceberg/auth_data/",
    "Columns": [
      {"Name": "auth_id", "Type": "string"},
      {"Name": "xid", "Type": "string"},
      {"Name": "auth_ts", "Type": "timestamp"},
      {"Name": "merchant", "Type": "string"},
      {"Name": "amount", "Type": "double"}
    ]
  }
}
EOF
)"

echo "Done. Glue table ${GLUE_DB}.${GLUE_TABLE} now points to ${METADATA_LOCATION}"
echo ""
echo "Test in Athena:"
echo '  SELECT * FROM "AwsDataCatalog"."pme-hackathon-iceberg-db"."auth_data" LIMIT 5;'
