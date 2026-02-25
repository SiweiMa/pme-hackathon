# -----------------------------------------------------------------------------
# Athena Spark workgroups — one per analyst persona
#
# Each workgroup uses Apache Spark engine v3. Query results are written to a
# per-workgroup prefix in the data bucket.
# -----------------------------------------------------------------------------

resource "aws_athena_workgroup" "fraud" {
  name = "pwe-hackathon-pme-fraud"

  configuration {
    engine_version {
      selected_engine_version = "PySpark engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.id}/athena-results/fraud/"
    }
  }
}

resource "aws_athena_workgroup" "marketing" {
  name = "pwe-hackathon-pme-marketing"

  configuration {
    engine_version {
      selected_engine_version = "PySpark engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.id}/athena-results/marketing/"
    }
  }
}

resource "aws_athena_workgroup" "junior" {
  name = "pwe-hackathon-pme-junior"

  configuration {
    engine_version {
      selected_engine_version = "PySpark engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.id}/athena-results/junior/"
    }
  }
}
