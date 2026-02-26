# -----------------------------------------------------------------------------
# KMS keys for Parquet Modular Encryption (PME)
# - footer-key: encrypts Parquet footer/schema (required for all readers)
# - pci-key:    encrypts PCI columns (SSN, PAN, card_number)
# - pii-key:    encrypts PII columns (name, email, phone)
# -----------------------------------------------------------------------------

locals {
  kms_key_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnableRootAccountAccess"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.aws_account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      }
    ]
  })
}

# --- Footer / Schema key ---------------------------------------------------

resource "aws_kms_key" "footer" {
  description         = "PME footer/schema encryption key"
  enable_key_rotation = true
  policy              = local.kms_key_policy
}

resource "aws_kms_alias" "footer" {
  name          = "alias/pme-hackathon-footer-key"
  target_key_id = aws_kms_key.footer.key_id
}

# --- PCI key ----------------------------------------------------------------

resource "aws_kms_key" "pci" {
  description         = "PME PCI column encryption key (SSN, PAN, card_number)"
  enable_key_rotation = true
  policy              = local.kms_key_policy
}

resource "aws_kms_alias" "pci" {
  name          = "alias/pme-hackathon-pci-key"
  target_key_id = aws_kms_key.pci.key_id
}

# --- PII key ----------------------------------------------------------------

resource "aws_kms_key" "pii" {
  description         = "PME PII column encryption key (name, email, phone)"
  enable_key_rotation = true
  policy              = local.kms_key_policy
}

resource "aws_kms_alias" "pii" {
  name          = "alias/pme-hackathon-pii-key"
  target_key_id = aws_kms_key.pii.key_id
}
