# -----------------------------------------------------------------------------
# Outputs — consumed by the PME pipeline and for verification
# -----------------------------------------------------------------------------

# KMS key ARNs
output "kms_footer_key_arn" {
  description = "ARN of the footer/schema encryption KMS key"
  value       = aws_kms_key.footer.arn
}

output "kms_pci_key_arn" {
  description = "ARN of the PCI column encryption KMS key"
  value       = aws_kms_key.pci.arn
}

output "kms_pii_key_arn" {
  description = "ARN of the PII column encryption KMS key"
  value       = aws_kms_key.pii.arn
}

# IAM role ARNs
output "role_fraud_analyst_arn" {
  description = "ARN of the fraud-analyst IAM role"
  value       = aws_iam_role.fraud_analyst.arn
}

output "role_marketing_analyst_arn" {
  description = "ARN of the marketing-analyst IAM role"
  value       = aws_iam_role.marketing_analyst.arn
}

output "role_junior_analyst_arn" {
  description = "ARN of the junior-analyst IAM role"
  value       = aws_iam_role.junior_analyst.arn
}

output "role_write_arn" {
  description = "ARN of the write-role IAM role"
  value       = aws_iam_role.write_role.arn
}

output "role_athena_spark_execution_arn" {
  description = "ARN of the Athena Spark execution role"
  value       = aws_iam_role.athena_spark_execution.arn
}

# S3
output "data_bucket_name" {
  description = "Name of the encrypted Parquet data bucket"
  value       = aws_s3_bucket.data.id
}

output "data_bucket_arn" {
  description = "ARN of the encrypted Parquet data bucket"
  value       = aws_s3_bucket.data.arn
}

# Athena workgroups
output "athena_workgroup_fraud" {
  description = "Athena Spark workgroup for fraud analyst"
  value       = aws_athena_workgroup.fraud.name
}

output "athena_workgroup_marketing" {
  description = "Athena Spark workgroup for marketing analyst"
  value       = aws_athena_workgroup.marketing.name
}

output "athena_workgroup_junior" {
  description = "Athena Spark workgroup for junior analyst"
  value       = aws_athena_workgroup.junior.name
}
