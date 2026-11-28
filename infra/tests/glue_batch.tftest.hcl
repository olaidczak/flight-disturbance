mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "592964949672"
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"*\",\"Resource\":\"*\"}]}"
    }
  }
}

mock_provider "archive" {
  mock_data "archive_file" {
    defaults = {
      output_path         = "../build/datalake_layers.zip"
      output_base64sha256 = "dGVzdA=="
    }
  }
}

run "glue_job_has_cost_limits" {
  command = plan

  assert {
    condition     = aws_glue_job.batch.glue_version == "5.0"
    error_message = "Glue 5.0 (Python 3.11) is required: schema.to_utc_iso needs datetime.fromisoformat from Python 3.11."
  }

  assert {
    condition     = aws_glue_job.batch.number_of_workers == 2 && aws_glue_job.batch.timeout <= 60 && aws_glue_job.batch.max_retries == 0
    error_message = "Nightly job must keep the minimum 2 workers, a timeout of at most 60 minutes and no automatic retries."
  }

  assert {
    condition     = aws_glue_job.batch.execution_property[0].max_concurrent_runs == 1
    error_message = "Only one run at a time, so concurrent runs cannot overwrite each other's partitions."
  }
}

run "nightly_trigger_is_disabled_by_default" {
  command = plan

  assert {
    condition     = aws_glue_trigger.nightly.enabled == false && aws_glue_trigger.nightly.schedule == "cron(0 2 * * ? *)"
    error_message = "The nightly trigger must be disabled by default and run at 02:00 UTC."
  }
}

run "nightly_trigger_can_be_enabled" {
  command = plan

  variables {
    glue_nightly_enabled = true
  }

  assert {
    condition     = aws_glue_trigger.nightly.enabled == true
    error_message = "glue_nightly_enabled = true must enable the trigger."
  }
}

run "raw_prefix_expires_and_archiver_writes_only_raw" {
  command = plan

  assert {
    condition     = one([for r in aws_s3_bucket_lifecycle_configuration.datalake.rule : r.expiration[0].days if r.id == "raw-expire"]) == 3
    error_message = "raw/ objects must expire after 3 days."
  }

  assert {
    condition     = aws_lambda_function.bronze_archiver.environment[0].variables["LAKE_BUCKET"] != null && !contains(keys(aws_lambda_function.bronze_archiver.environment[0].variables), "BRONZE_WRITE_ONCE")
    error_message = "The archiver needs LAKE_BUCKET and no longer uses BRONZE_WRITE_ONCE."
  }
}
