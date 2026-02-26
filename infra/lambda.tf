# -----------------------------------------------------------------------------
# Lambda container image infrastructure for PME encryption pipeline
#
# Resources:
#   - ECR repository for the Lambda container image
#   - CloudWatch log group for Lambda execution logs
#   - IAM execution role with KMS, S3, and CloudWatch policies
#   - Lambda function (container image)
# -----------------------------------------------------------------------------

# --- ECR repository -----------------------------------------------------------

resource "aws_ecr_repository" "pme_lambda" {
  name                 = "pwe-hackathon-pme-lambda"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_lifecycle_policy" "pme_lambda" {
  repository = aws_ecr_repository.pme_lambda.name
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

resource "aws_cloudwatch_log_group" "pme_lambda" {
  name              = "/aws/lambda/pwe-hackathon-pme-encrypt"
  retention_in_days = 7
}

# --- IAM execution role -------------------------------------------------------

resource "aws_iam_role" "lambda_pme_encrypt" {
  name = "pwe-hackathon-lambda-pme-encrypt"
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

resource "aws_iam_role_policy" "lambda_pme_kms" {
  name = "kms-encrypt-all"
  role = aws_iam_role.lambda_pme_encrypt.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "KMSEncryptAllPMEKeys"
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

resource "aws_iam_role_policy" "lambda_pme_s3" {
  name = "s3-read-write"
  role = aws_iam_role.lambda_pme_encrypt.id
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

resource "aws_iam_role_policy" "lambda_pme_logs" {
  name = "cloudwatch-logs"
  role = aws_iam_role.lambda_pme_encrypt.id
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
        Resource = "${aws_cloudwatch_log_group.pme_lambda.arn}:*"
      }
    ]
  })
}

# --- Lambda function ----------------------------------------------------------

resource "aws_lambda_function" "pme_encrypt" {
  function_name = "pwe-hackathon-pme-encrypt"
  role          = aws_iam_role.lambda_pme_encrypt.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.pme_lambda.repository_url}:latest"
  memory_size   = 1536
  timeout       = 300

  logging_config {
    log_group  = aws_cloudwatch_log_group.pme_lambda.name
    log_format = "Text"
  }

  environment {
    variables = {
      S3_BUCKET      = aws_s3_bucket.data.id
      S3_PREFIX      = "pme-data"
      FOOTER_KEY_ARN = aws_kms_alias.footer.arn
      PCI_KEY_ARN    = aws_kms_alias.pci.arn
      PII_KEY_ARN    = aws_kms_alias.pii.arn
    }
  }

  lifecycle {
    ignore_changes = [image_uri]
  }
}

# --- S3 event trigger ---------------------------------------------------------

resource "aws_lambda_permission" "s3_trigger" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.pme_encrypt.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data.arn
}

resource "aws_s3_bucket_notification" "csv_upload" {
  bucket = aws_s3_bucket.data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.pme_encrypt.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw-data/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.s3_trigger]
}

# --- Outputs ------------------------------------------------------------------

output "ecr_repository_url" {
  description = "ECR repository URL for the PME Lambda image"
  value       = aws_ecr_repository.pme_lambda.repository_url
}

output "lambda_function_name" {
  description = "Name of the PME encryption Lambda function"
  value       = aws_lambda_function.pme_encrypt.function_name
}

output "lambda_function_arn" {
  description = "ARN of the PME encryption Lambda function"
  value       = aws_lambda_function.pme_encrypt.arn
}

output "lambda_execution_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_pme_encrypt.arn
}
