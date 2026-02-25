# -----------------------------------------------------------------------------
# Glue Data Catalog — PME table metadata
#
# Registers the PME-encrypted Parquet data as an external table in the Glue
# Catalog.  Table metadata only — no decrypted files stored.  The encrypted
# Parquet stays in S3 untouched.
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_database" "pme" {
  name = "${var.project}-pme-db"

  description = "Glue database for PME-encrypted customer data (hackathon)"
}

resource "aws_glue_catalog_table" "customer_data" {
  database_name = aws_glue_catalog_database.pme.name
  name          = "customer_data"
  description   = "PME-encrypted customer data (Parquet with column-level KMS encryption)"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"       = "parquet"
    "parquet.compression"  = "SNAPPY"
    "EXTERNAL"             = "TRUE"
    "has_encrypted_data"   = "true"
    "pme.footer_key"       = aws_kms_key.footer.arn
    "pme.pci_key"          = aws_kms_key.pci.arn
    "pme.pii_key"          = aws_kms_key.pii.arn
    "pme.plaintext_footer" = "true"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/pme-data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    # Schema matches the CSV columns; encryption status noted in comments.
    # PII columns: first_name, last_name, email  (encrypted with PII KMS key)
    # PCI columns: ssn                            (encrypted with PCI KMS key)
    # Plaintext:   xid, balance                   (not encrypted)

    columns {
      name    = "first_name"
      type    = "string"
      comment = "PII — encrypted with PII KMS key"
    }

    columns {
      name    = "last_name"
      type    = "string"
      comment = "PII — encrypted with PII KMS key"
    }

    columns {
      name    = "ssn"
      type    = "string"
      comment = "PCI — encrypted with PCI KMS key"
    }

    columns {
      name    = "email"
      type    = "string"
      comment = "PII — encrypted with PII KMS key"
    }

    columns {
      name    = "xid"
      type    = "string"
      comment = "Public identifier — not encrypted"
    }

    columns {
      name    = "balance"
      type    = "double"
      comment = "Account balance — not encrypted"
    }
  }
}
