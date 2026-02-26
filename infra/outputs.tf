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

# Glue Data Catalog
output "glue_database_name" {
  description = "Glue catalog database for PME-encrypted data"
  value       = aws_glue_catalog_database.pme.name
}

output "glue_table_name" {
  description = "Glue catalog table for PME-encrypted customer data"
  value       = aws_glue_catalog_table.customer_data.name
}

# Federated Connector
output "connector_ecr_repository_url" {
  description = "ECR repository URL for the federated connector image"
  value       = aws_ecr_repository.pme_connector.repository_url
}

output "connector_lambda_function_name" {
  description = "Name of the federated connector Lambda function"
  value       = aws_lambda_function.pme_connector.function_name
}

output "connector_lambda_function_arn" {
  description = "ARN of the federated connector Lambda function"
  value       = aws_lambda_function.pme_connector.arn
}

output "athena_data_catalog_name" {
  description = "Athena data catalog for federated PME queries"
  value       = aws_athena_data_catalog.pme_connector.name
}

output "athena_spill_bucket" {
  description = "S3 bucket for Athena Federation spill data"
  value       = aws_s3_bucket.athena_spill.id
}

# Snowflake External Function
output "sf_decrypt_ecr_repository_url" {
  description = "ECR repository URL for the Snowflake decrypt Lambda image"
  value       = aws_ecr_repository.sf_decrypt.repository_url
}

output "sf_decrypt_lambda_function_name" {
  description = "Name of the Snowflake decrypt Lambda function"
  value       = aws_lambda_function.sf_decrypt.function_name
}

output "sf_decrypt_api_gateway_invoke_url" {
  description = "API Gateway invoke URL for Snowflake External Function (use in CREATE API INTEGRATION)"
  value       = "${aws_api_gateway_stage.prod.invoke_url}/decrypt"
}

output "sf_snowflake_role_arn" {
  description = "IAM role ARN for Snowflake to assume (use in CREATE API INTEGRATION)"
  value       = aws_iam_role.snowflake_apigw.arn
}
