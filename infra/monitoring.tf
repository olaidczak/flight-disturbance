############ Rejestr monitorowanych modułów i budżet Free Tier ############
locals {
  # Centralny rejestr funkcji Lambda objętych alarmami błędów i wykresami na dashboardzie.
  # Dopisanie kolejnej funkcji (1 wpis) automatycznie tworzy dla niej alarm i dodaje ją do panelu.
  monitored_lambdas = merge(
    { for k, fn in aws_lambda_function.poller : k => fn.function_name },
    {
      bronze_archiver  = aws_lambda_function.bronze_archiver.function_name
      silver_transform = aws_lambda_function.silver_transform.function_name
      speed_consumer   = aws_lambda_function.speed_consumer.function_name
    }
  )

  alarm_actions = var.alarm_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  all_alarm_arns = concat(
    [
      aws_cloudwatch_metric_alarm.sqs_dlq_messages.arn,
      aws_cloudwatch_metric_alarm.sqs_queue_depth.arn,
      aws_cloudwatch_metric_alarm.sqs_oldest_message_age.arn,
    ],
    [for k in sort(keys(local.monitored_lambdas)) : aws_cloudwatch_metric_alarm.lambda_errors[k].arn],
    [
      aws_cloudwatch_metric_alarm.seismic_ws_log_errors.arn,
    ]
  )

  all_alarm_names = concat(
    [
      aws_cloudwatch_metric_alarm.sqs_dlq_messages.alarm_name,
      aws_cloudwatch_metric_alarm.sqs_queue_depth.alarm_name,
      aws_cloudwatch_metric_alarm.sqs_oldest_message_age.alarm_name,
    ],
    [for k in sort(keys(local.monitored_lambdas)) : aws_cloudwatch_metric_alarm.lambda_errors[k].alarm_name],
    [
      aws_cloudwatch_metric_alarm.seismic_ws_log_errors.alarm_name,
    ]
  )

  total_alarm_count = 3 + length(local.monitored_lambdas) + 1
}

check "cloudwatch_free_tier_alarm_budget" {
  assert {
    condition     = local.total_alarm_count <= 10
    error_message = "CloudWatch alarm count (${local.total_alarm_count}) exceeds the AWS Free Tier limit (10 alarms)."
  }
}

############ Optional Email Notification Channel (SNS) ############
# Avoid alias/aws/sns (blocks CloudWatch Alarms from publishing) and paid CMK ($1/month).
resource "aws_sns_topic" "alarms" {
  count = var.alarm_email != "" ? 1 : 0
  name  = "${var.project}-alarms"
}

data "aws_iam_policy_document" "alarms_sns_topic" {
  count = var.alarm_email != "" ? 1 : 0

  statement {
    sid     = "AllowCloudWatchAlarmsPublish"
    effect  = "Allow"
    actions = ["SNS:Publish"]

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    resources = [aws_sns_topic.alarms[0].arn]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:cloudwatch:${var.region}:${data.aws_caller_identity.current.account_id}:alarm:${var.project}-*"]
    }
  }
}

resource "aws_sns_topic_policy" "alarms" {
  count  = var.alarm_email != "" ? 1 : 0
  arn    = aws_sns_topic.alarms[0].arn
  policy = data.aws_iam_policy_document.alarms_sns_topic[0].json
}

resource "aws_sns_topic_subscription" "alarms_email" {
  count     = var.alarm_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alarms[0].arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

############ CloudWatch Log Metric Filter for SeismicPortal WebSocket ############
resource "aws_cloudwatch_log_metric_filter" "seismic_ws_errors" {
  name           = "${var.project}-seismic-ws-errors"
  log_group_name = aws_cloudwatch_log_group.seismic_ws.name
  pattern        = "?ERROR ?CRITICAL ?Traceback"

  metric_transformation {
    name      = "LogErrors"
    namespace = "FlightDisruption/SeismicWs"
    value     = "1"
    unit      = "Count"
  }
}

############ CloudWatch Metric Alarms (Standard Resolution 300s, Free Tier) ############
resource "aws_cloudwatch_metric_alarm" "sqs_dlq_messages" {
  alarm_name          = "${var.project}-sqs-dlq-messages"
  alarm_description   = "At least ${var.alarm_dlq_messages_threshold} message(s) detected in the SQS FIFO Dead-Letter Queue (DLQ)."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.alarm_dlq_messages_threshold
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.speed_events_dlq.name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "sqs_queue_depth" {
  alarm_name          = "${var.project}-sqs-queue-depth"
  alarm_description   = "Main SQS FIFO queue depth exceeded ${var.alarm_sqs_depth_threshold} visible messages for 2 consecutive 5-minute periods."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.alarm_sqs_depth_threshold
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.speed_events.name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "sqs_oldest_message_age" {
  alarm_name          = "${var.project}-sqs-oldest-message-age"
  alarm_description   = "Age of the oldest message in the main SQS FIFO queue exceeded ${var.alarm_sqs_age_threshold_seconds} seconds."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.alarm_sqs_age_threshold_seconds
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.speed_events.name
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = local.monitored_lambdas

  alarm_name          = "${var.project}-lambda-${each.key}-errors"
  alarm_description   = "Lambda function ${each.value} reported at least ${var.alarm_lambda_errors_threshold} execution error(s) within 5 minutes."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = var.alarm_lambda_errors_threshold
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = each.value
  }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "seismic_ws_log_errors" {
  alarm_name          = "${var.project}-seismic-ws-log-errors"
  alarm_description   = "Seismic WebSocket service logged at least ${var.alarm_seismic_errors_threshold} error(s) within 5 minutes."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "LogErrors"
  namespace           = "FlightDisruption/SeismicWs"
  period              = 300
  statistic           = "Sum"
  threshold           = var.alarm_seismic_errors_threshold
  treat_missing_data  = "notBreaching"

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
}

############ Zbiorczy panel operacyjny (CloudWatch Dashboard) ############
resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${var.project}-pipeline"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "alarm"
        x      = 0
        y      = 0
        width  = 24
        height = 3
        properties = {
          title  = "Pipeline Alarm Status"
          alarms = local.all_alarm_arns
          sortBy = "stateUpdatedTimestamp"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 3
        width  = 12
        height = 6
        properties = {
          title  = "SQS Main Queue Traffic & Backlog"
          region = var.region
          view   = "timeSeries"
          period = 300
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.speed_events.name, { stat = "Maximum" }],
            [".", "ApproximateNumberOfMessagesNotVisible", ".", ".", { stat = "Maximum" }],
            [".", "NumberOfMessagesSent", ".", ".", { stat = "Sum" }],
            [".", "NumberOfMessagesDeleted", ".", ".", { stat = "Sum" }]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 3
        width  = 12
        height = 6
        properties = {
          title  = "SQS Message Age & DLQ Activity"
          region = var.region
          view   = "timeSeries"
          period = 300
          metrics = [
            ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", aws_sqs_queue.speed_events.name, { stat = "Maximum" }],
            [".", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.speed_events_dlq.name, { stat = "Maximum" }],
            [".", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.archive_events_dlq.name, { stat = "Maximum" }],
            [".", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.silver_transform_dlq.name, { stat = "Maximum" }]
          ]
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 9
        width  = 12
        height = 6
        properties = {
          title  = "Lambda Invocations & Errors"
          region = var.region
          view   = "timeSeries"
          period = 300
          metrics = concat([
            for k in sort(keys(local.monitored_lambdas)) : [
              ["AWS/Lambda", "Invocations", "FunctionName", local.monitored_lambdas[k], { stat = "Sum" }],
              ["AWS/Lambda", "Errors", "FunctionName", local.monitored_lambdas[k], { stat = "Sum" }]
            ]
          ]...)
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 9
        width  = 12
        height = 6
        properties = {
          title  = "Lambda Duration & Throttles"
          region = var.region
          view   = "timeSeries"
          period = 300
          metrics = concat([
            for k in sort(keys(local.monitored_lambdas)) : [
              ["AWS/Lambda", "Duration", "FunctionName", local.monitored_lambdas[k], { stat = "Maximum" }],
              ["AWS/Lambda", "Throttles", "FunctionName", local.monitored_lambdas[k], { stat = "Sum" }]
            ]
          ]...)
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 15
        width  = 12
        height = 6
        properties = {
          title  = "Seismic WebSocket Log Errors"
          region = var.region
          view   = "timeSeries"
          period = 300
          metrics = [
            ["FlightDisruption/SeismicWs", "LogErrors", { stat = "Sum" }]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 15
        width  = 12
        height = 6
        properties = {
          title  = "Seismic WebSocket ECS Utilization"
          region = var.region
          view   = "timeSeries"
          period = 300
          metrics = [
            ["AWS/ECS", "CPUUtilization", "ClusterName", aws_ecs_cluster.main.name, "ServiceName", aws_ecs_service.seismic_ws.name, { stat = "Average" }],
            [".", "MemoryUtilization", ".", ".", ".", ".", { stat = "Average" }]
          ]
        }
      }
    ]
  })
}
