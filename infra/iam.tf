# -----------------------------------------------------------------------------
# IAM roles for PME column-level access control
#
# Trust policy: allows any principal in the same account to assume the role.
# Each role gets a KMS policy (decrypt specific keys) and an S3 policy.
# -----------------------------------------------------------------------------

locals {
  trust_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.aws_account_id}:root" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

# =============================================================================
# fraud-analyst: decrypt ALL 3 keys + S3 read
# =============================================================================

resource "aws_iam_role" "fraud_analyst" {
  name               = "pwe-hackathon-fraud-analyst"
  assume_role_policy = local.trust_policy
}

resource "aws_iam_role_policy" "fraud_analyst_kms" {
  name = "kms-decrypt-all"
  role = aws_iam_role.fraud_analyst.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DecryptAllPMEKeys"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
        ]
        Resource = [
          aws_kms_key.footer.arn,
          aws_kms_key.pci.arn,
          aws_kms_key.pii.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "fraud_analyst_s3" {
  name = "s3-read"
  role = aws_iam_role.fraud_analyst.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadData"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      }
    ]
  })
}

# =============================================================================
# marketing-analyst: decrypt footer + PII only + S3 read
# =============================================================================

resource "aws_iam_role" "marketing_analyst" {
  name               = "pwe-hackathon-marketing-analyst"
  assume_role_policy = local.trust_policy
}

resource "aws_iam_role_policy" "marketing_analyst_kms" {
  name = "kms-decrypt-footer-pii"
  role = aws_iam_role.marketing_analyst.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DecryptFooterAndPII"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
        ]
        Resource = [
          aws_kms_key.footer.arn,
          aws_kms_key.pii.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "marketing_analyst_s3" {
  name = "s3-read"
  role = aws_iam_role.marketing_analyst.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadData"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      }
    ]
  })
}

# =============================================================================
# junior-analyst: decrypt footer only + S3 read
# =============================================================================

resource "aws_iam_role" "junior_analyst" {
  name               = "pwe-hackathon-junior-analyst"
  assume_role_policy = local.trust_policy
}

resource "aws_iam_role_policy" "junior_analyst_kms" {
  name = "kms-decrypt-footer"
  role = aws_iam_role.junior_analyst.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DecryptFooterOnly"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
        ]
        Resource = [
          aws_kms_key.footer.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "junior_analyst_s3" {
  name = "s3-read"
  role = aws_iam_role.junior_analyst.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadData"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      }
    ]
  })
}

# =============================================================================
# write-role: encrypt ALL 3 keys + S3 read/write
# =============================================================================

resource "aws_iam_role" "write_role" {
  name               = "pwe-hackathon-write-role"
  assume_role_policy = local.trust_policy
}

resource "aws_iam_role_policy" "write_role_kms" {
  name = "kms-encrypt-all"
  role = aws_iam_role.write_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EncryptAllPMEKeys"
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
        ]
        Resource = [
          aws_kms_key.footer.arn,
          aws_kms_key.pci.arn,
          aws_kms_key.pii.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "write_role_s3" {
  name = "s3-read-write"
  role = aws_iam_role.write_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadWriteData"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      }
    ]
  })
}

# =============================================================================
# athena-spark-execution: shared execution role for Athena Spark workgroups
# =============================================================================

resource "aws_iam_role" "athena_spark_execution" {
  name = "pwe-hackathon-athena-spark-execution"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "athena.amazonaws.com" }
        Action    = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = var.aws_account_id
          }
          ArnLike = {
            "aws:SourceArn" = "arn:aws:athena:${var.aws_region}:${var.aws_account_id}:workgroup/pwe-hackathon-pme-*"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "athena_spark_s3" {
  name = "s3-data-and-results"
  role = aws_iam_role.athena_spark_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DataAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "athena_spark_athena" {
  name = "athena-access"
  role = aws_iam_role.athena_spark_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:GetWorkGroup",
          "athena:TerminateSession",
          "athena:GetSession",
          "athena:GetSessionStatus",
          "athena:ListSessions",
          "athena:StartCalculationExecution",
          "athena:GetCalculationExecutionCode",
          "athena:StopCalculationExecution",
          "athena:ListCalculationExecutions",
          "athena:GetCalculationExecution",
          "athena:GetCalculationExecutionStatus",
          "athena:ListExecutors",
          "athena:ExportNotebook",
          "athena:UpdateNotebook",
          "athena:CreatePresignedNotebookUrl",
        ]
        Resource = "arn:aws:athena:${var.aws_region}:${var.aws_account_id}:workgroup/pwe-hackathon-pme-*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "athena_spark_glue" {
  name = "glue-catalog"
  role = aws_iam_role.athena_spark_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateDatabase",
          "glue:CreateTable",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "athena_spark_cloudwatch" {
  name = "cloudwatch-logs"
  role = aws_iam_role.athena_spark_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogStreams",
          "logs:DescribeLogGroups",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:*"
      },
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "AmazonAthenaForApacheSpark"
          }
        }
      }
    ]
  })
}
