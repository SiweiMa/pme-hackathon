# -----------------------------------------------------------------------------
# Athena Federated Connector — Lambda decryption proxy for PME data
#
# Resources:
#   - ECR repository for the connector container image
#   - CloudWatch log group for Lambda execution logs
#   - S3 spill bucket (required by Athena Federation protocol)
#   - IAM execution role with KMS, S3, Glue, and CloudWatch policies
#   - Lambda function (container image)
#   - Athena data catalog (LAMBDA type, pointing to connector)
#   - Lambda permission for Athena invocation
# -----------------------------------------------------------------------------

# --- ECR repository -----------------------------------------------------------

resource "aws_ecr_repository" "pme_connector" {
  name                 = "pwe-hackathon-pme-connector"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_lifecycle_policy" "pme_connector" {
  repository = aws_ecr_repository.pme_connector.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# --- CloudWatch log group -----------------------------------------------------

resource "aws_cloudwatch_log_group" "pme_connector" {
  name              = "/aws/lambda/pwe-hackathon-pme-connector"
  retention_in_days = 7
}

# --- S3 spill bucket (required by Athena Federation) --------------------------

resource "aws_s3_bucket" "athena_spill" {
  bucket = "pwe-hackathon-athena-spill-${var.aws_account_id}"
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_spill" {
  bucket = aws_s3_bucket.athena_spill.id

  rule {
    id     = "expire-spill-data"
    status = "Enabled"

    filter {}

    expiration {
      days = 1
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_spill" {
  bucket = aws_s3_bucket.athena_spill.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- IAM execution role -------------------------------------------------------

resource "aws_iam_role" "lambda_pme_connector" {
  name = "pwe-hackathon-lambda-pme-connector"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

# KMS: Decrypt all 3 keys (full access — RBAC is application-level)
resource "aws_iam_role_policy" "connector_kms" {
  name = "kms-decrypt-all"
  role = aws_iam_role.lambda_pme_connector.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KMSDecryptAllPMEKeys"
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

# S3: Read data bucket
resource "aws_iam_role_policy" "connector_s3_data" {
  name = "s3-read-data"
  role = aws_iam_role.lambda_pme_connector.id
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

# S3: Read/Write/Delete spill bucket
resource "aws_iam_role_policy" "connector_s3_spill" {
  name = "s3-spill"
  role = aws_iam_role.lambda_pme_connector.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3SpillAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.athena_spill.arn,
          "${aws_s3_bucket.athena_spill.arn}/*",
        ]
      }
    ]
  })
}

# CloudWatch logs
resource "aws_iam_role_policy" "connector_logs" {
  name = "cloudwatch-logs"
  role = aws_iam_role.lambda_pme_connector.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "${aws_cloudwatch_log_group.pme_connector.arn}:*"
      }
    ]
  })
}

# Glue: Read catalog metadata
resource "aws_iam_role_policy" "connector_glue" {
  name = "glue-catalog-read"
  role = aws_iam_role.lambda_pme_connector.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogRead"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
        ]
        Resource = "*"
      }
    ]
  })
}

# --- Lambda function ----------------------------------------------------------

resource "aws_lambda_function" "pme_connector" {
  function_name = "pwe-hackathon-pme-connector"
  role          = aws_iam_role.lambda_pme_connector.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.pme_connector.repository_url}:latest"
  memory_size   = 3008
  timeout       = 300

  logging_config {
    log_group  = aws_cloudwatch_log_group.pme_connector.name
    log_format = "Text"
  }

  environment {
    variables = {
      S3_BUCKET      = aws_s3_bucket.data.id
      S3_PREFIX      = "pme-data"
      FOOTER_KEY_ARN = aws_kms_alias.footer.arn
      PCI_KEY_ARN    = aws_kms_alias.pci.arn
      PII_KEY_ARN    = aws_kms_alias.pii.arn
      SPILL_BUCKET   = aws_s3_bucket.athena_spill.id
      SPILL_PREFIX   = "athena-spill"
    }
  }

  lifecycle {
    ignore_changes = [image_uri]
  }
}

# --- Lambda permission for Athena invocation ----------------------------------

resource "aws_lambda_permission" "athena_invoke" {
  statement_id  = "AllowAthenaInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.pme_connector.function_name
  principal     = "athena.amazonaws.com"
}

# --- Athena data catalog (LAMBDA type) ----------------------------------------

resource "aws_athena_data_catalog" "pme_connector" {
  name        = "pwe-hackathon-pme-connector"
  description = "Federated connector for PME-encrypted customer data with RBAC"
  type        = "LAMBDA"

  parameters = {
    "function" = aws_lambda_function.pme_connector.arn
  }
}

# --- Athena SQL workgroup for federated queries ------------------------------

resource "aws_athena_workgroup" "federated" {
  name = "pwe-hackathon-pme-federated"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_spill.id}/query-results/"
    }
  }
}
