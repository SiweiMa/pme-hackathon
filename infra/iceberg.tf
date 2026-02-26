# -----------------------------------------------------------------------------
# Snowflake-managed Iceberg table infrastructure
#
# Resources:
#   - S3 bucket for Iceberg data + metadata (written by Snowflake)
#   - IAM role for Snowflake storage integration (assume-role trust)
#   - Glue catalog database + Iceberg table registration (for Athena access)
# -----------------------------------------------------------------------------

# --- S3 bucket for Iceberg data ---------------------------------------------

resource "aws_s3_bucket" "iceberg" {
  bucket = "pme-hackathon-iceberg-${var.aws_account_id}"
}

resource "aws_s3_bucket_versioning" "iceberg" {
  bucket = aws_s3_bucket.iceberg.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "iceberg" {
  bucket = aws_s3_bucket.iceberg.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- IAM role for Snowflake storage integration ------------------------------
#
# After running CREATE STORAGE INTEGRATION in Snowflake, update this role's
# trust policy with STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
# from DESCRIBE STORAGE INTEGRATION.  Same bootstrap pattern as snowflake.tf.

resource "aws_iam_role" "snowflake_iceberg" {
  name = "pme-hackathon-snowflake-iceberg"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.aws_account_id}:root" }
        Action    = "sts:AssumeRole"
        Condition = {}
      }
    ]
  })
}

resource "aws_iam_role_policy" "snowflake_iceberg_s3" {
  name = "s3-iceberg-readwrite"
  role = aws_iam_role.snowflake_iceberg.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = aws_s3_bucket.iceberg.arn
      },
      {
        Sid    = "S3ReadWriteObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
        ]
        Resource = "${aws_s3_bucket.iceberg.arn}/*"
      }
    ]
  })
}

# --- Glue catalog for Athena Iceberg access ----------------------------------

resource "aws_glue_catalog_database" "iceberg" {
  name        = "pme-hackathon-iceberg-db"
  description = "Glue database for Snowflake-managed Iceberg tables (hackathon)"
}
