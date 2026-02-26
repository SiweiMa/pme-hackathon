# -----------------------------------------------------------------------------
# Snowflake External Function — Lambda decryption proxy for PME data
#
# Snowflake calls this via API Gateway (IAM auth) to decrypt PME-encrypted
# Parquet data with column-level RBAC null-masking.
#
# Resources:
#   - ECR repository for the Lambda container image
#   - CloudWatch log group for Lambda execution logs
#   - IAM execution role with KMS, S3, and CloudWatch policies
#   - Lambda function (container image)
#   - API Gateway REST API with POST /decrypt (IAM auth)
#   - IAM role for Snowflake to assume when calling API Gateway
# -----------------------------------------------------------------------------

# --- ECR repository -----------------------------------------------------------

resource "aws_ecr_repository" "sf_decrypt" {
  name                 = "pme-hackathon-sf-decrypt"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

resource "aws_ecr_lifecycle_policy" "sf_decrypt" {
  repository = aws_ecr_repository.sf_decrypt.name
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

resource "aws_cloudwatch_log_group" "sf_decrypt" {
  name              = "/aws/lambda/pme-hackathon-sf-decrypt"
  retention_in_days = 7
}

# --- IAM execution role for Lambda --------------------------------------------

resource "aws_iam_role" "lambda_sf_decrypt" {
  name = "pme-hackathon-lambda-sf-decrypt"
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
resource "aws_iam_role_policy" "sf_decrypt_kms" {
  name = "kms-decrypt-all"
  role = aws_iam_role.lambda_sf_decrypt.id
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
resource "aws_iam_role_policy" "sf_decrypt_s3_data" {
  name = "s3-read-data"
  role = aws_iam_role.lambda_sf_decrypt.id
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

# CloudWatch logs
resource "aws_iam_role_policy" "sf_decrypt_logs" {
  name = "cloudwatch-logs"
  role = aws_iam_role.lambda_sf_decrypt.id
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
        Resource = "${aws_cloudwatch_log_group.sf_decrypt.arn}:*"
      }
    ]
  })
}

# --- Lambda function ----------------------------------------------------------

resource "aws_lambda_function" "sf_decrypt" {
  function_name = "pme-hackathon-sf-decrypt"
  role          = aws_iam_role.lambda_sf_decrypt.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.sf_decrypt.repository_url}:latest"
  memory_size   = 3008
  timeout       = 300

  logging_config {
    log_group  = aws_cloudwatch_log_group.sf_decrypt.name
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

# --- API Gateway REST API -----------------------------------------------------

resource "aws_api_gateway_rest_api" "sf_decrypt" {
  name        = "pme-hackathon-sf-decrypt"
  description = "Snowflake External Function proxy for PME decryption"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

# POST /decrypt resource
resource "aws_api_gateway_resource" "decrypt" {
  rest_api_id = aws_api_gateway_rest_api.sf_decrypt.id
  parent_id   = aws_api_gateway_rest_api.sf_decrypt.root_resource_id
  path_part   = "decrypt"
}

resource "aws_api_gateway_method" "decrypt_post" {
  rest_api_id   = aws_api_gateway_rest_api.sf_decrypt.id
  resource_id   = aws_api_gateway_resource.decrypt.id
  http_method   = "POST"
  authorization = "AWS_IAM"
}

resource "aws_api_gateway_integration" "decrypt_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.sf_decrypt.id
  resource_id             = aws_api_gateway_resource.decrypt.id
  http_method             = aws_api_gateway_method.decrypt_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.sf_decrypt.invoke_arn
}

# --- API Gateway deployment + stage -------------------------------------------

resource "aws_api_gateway_deployment" "sf_decrypt" {
  rest_api_id = aws_api_gateway_rest_api.sf_decrypt.id

  depends_on = [
    aws_api_gateway_integration.decrypt_lambda,
  ]

  # Force new deployment when integration changes
  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.decrypt.id,
      aws_api_gateway_method.decrypt_post.id,
      aws_api_gateway_integration.decrypt_lambda.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "prod" {
  deployment_id = aws_api_gateway_deployment.sf_decrypt.id
  rest_api_id   = aws_api_gateway_rest_api.sf_decrypt.id
  stage_name    = "prod"
}

# --- Lambda permission for API Gateway invocation -----------------------------

resource "aws_lambda_permission" "apigw_invoke_sf_decrypt" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sf_decrypt.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.sf_decrypt.execution_arn}/*/*"
}

# --- IAM role for Snowflake to assume when calling API Gateway ----------------
#
# After running CREATE API INTEGRATION in Snowflake, you must update this
# role's trust policy with the API_AWS_IAM_USER_ARN and API_AWS_EXTERNAL_ID
# from DESCRIBE API INTEGRATION. See snowflake/setup.sql for details.

resource "aws_iam_role" "snowflake_apigw" {
  name = "pme-hackathon-snowflake-apigw"

  # Initial trust policy — allows the AWS account to assume this role.
  # After Snowflake bootstrap, update with the Snowflake principal ARN
  # and external ID from DESCRIBE API INTEGRATION.
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

resource "aws_iam_role_policy" "snowflake_apigw_invoke" {
  name = "apigw-invoke"
  role = aws_iam_role.snowflake_apigw.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokeAPIGateway"
        Effect = "Allow"
        Action = "execute-api:Invoke"
        Resource = "${aws_api_gateway_rest_api.sf_decrypt.execution_arn}/prod/POST/decrypt"
      }
    ]
  })
}
