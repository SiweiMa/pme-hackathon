variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-2"
}

variable "aws_account_id" {
  description = "AWS account ID (used in IAM trust policies)"
  type        = string
}

variable "project" {
  description = "Project name used for naming and tagging"
  type        = string
  default     = "pme-hackathon"
}

variable "data_bucket_name" {
  description = "S3 bucket for encrypted Parquet data"
  type        = string
  default     = "pwe-hackathon-pme-data-651767347247"
}

variable "state_bucket_name" {
  description = "S3 bucket for Terraform state (bootstrapped outside TF)"
  type        = string
  default     = "pme-hackathon-tf-state-651767347247"
}
