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
