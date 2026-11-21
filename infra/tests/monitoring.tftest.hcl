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

run "default_monitoring_within_free_tier" {
  command = plan

  assert {
    condition     = local.total_alarm_count == 10 && local.total_alarm_count <= 10
    error_message = "Default configuration must create 10 alarms (<= 10 within AWS Free Tier)."
  }

  assert {
    condition     = length(aws_sns_topic.alarms) == 0 && length(aws_sns_topic_subscription.alarms_email) == 0
    error_message = "When alarm_email is empty, no SNS topic or subscription should be created."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.sqs_dlq_messages.treat_missing_data == "notBreaching" &&
      aws_cloudwatch_metric_alarm.sqs_queue_depth.treat_missing_data == "notBreaching" &&
      aws_cloudwatch_metric_alarm.sqs_oldest_message_age.treat_missing_data == "notBreaching" &&
      aws_cloudwatch_metric_alarm.seismic_ws_log_errors.treat_missing_data == "notBreaching" &&
      alltrue([for a in aws_cloudwatch_metric_alarm.lambda_errors : a.treat_missing_data == "notBreaching"])
    )
    error_message = "All alarms must set treat_missing_data = 'notBreaching' to avoid false positives for idle components."
  }

  assert {
    condition     = aws_cloudwatch_log_metric_filter.seismic_ws_errors.pattern == "?ERROR ?CRITICAL ?Traceback"
    error_message = "Seismic WS log metric filter must match '?ERROR ?CRITICAL ?Traceback'."
  }

  assert {
    condition     = aws_cloudwatch_dashboard.pipeline.dashboard_name == "flight-disruption-pipeline"
    error_message = "Operational dashboard must be named flight-disruption-pipeline."
  }
}

run "optional_email_enables_sns_notifications" {
  command = plan

  variables {
    alarm_email = "operator@example.com"
  }

  assert {
    condition     = length(aws_sns_topic.alarms) == 1 && length(aws_sns_topic_subscription.alarms_email) == 1
    error_message = "Providing alarm_email must create an SNS topic and email subscription."
  }

  assert {
    condition     = aws_sns_topic_subscription.alarms_email[0].endpoint == "operator@example.com"
    error_message = "SNS subscription endpoint must match var.alarm_email."
  }
}

run "rejects_invalid_email_and_thresholds" {
  command = plan

  variables {
    alarm_email                     = "not-an-email"
    alarm_dlq_messages_threshold    = 0
    alarm_sqs_depth_threshold       = 0
    alarm_sqs_age_threshold_seconds = 10
    alarm_lambda_errors_threshold   = 0
    alarm_seismic_errors_threshold  = 0
  }

  expect_failures = [
    var.alarm_email,
    var.alarm_dlq_messages_threshold,
    var.alarm_sqs_depth_threshold,
    var.alarm_sqs_age_threshold_seconds,
    var.alarm_lambda_errors_threshold,
    var.alarm_seismic_errors_threshold,
  ]
}
