# -----------------------------------------------------------------------------
# Athena Spark workgroups — one per analyst persona
#
# Each workgroup uses Apache Spark engine v3. Query results are written to a
# per-workgroup prefix in the data bucket. All share a common execution role.
# -----------------------------------------------------------------------------

resource "aws_athena_workgroup" "fraud" {
  name          = "pme-hackathon-pme-fraud"
  force_destroy = true

  configuration {
    execution_role = aws_iam_role.athena_spark_execution.arn

    engine_version {
      selected_engine_version = "PySpark engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.id}/athena-results/fraud/"
    }
  }
}

resource "aws_athena_workgroup" "marketing" {
  name          = "pme-hackathon-pme-marketing"
  force_destroy = true

  configuration {
    execution_role = aws_iam_role.athena_spark_execution.arn

    engine_version {
      selected_engine_version = "PySpark engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.id}/athena-results/marketing/"
    }
  }
}

resource "aws_athena_workgroup" "junior" {
  name          = "pme-hackathon-pme-junior"
  force_destroy = true

  configuration {
    execution_role = aws_iam_role.athena_spark_execution.arn

    engine_version {
      selected_engine_version = "PySpark engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.id}/athena-results/junior/"
    }
  }
}
