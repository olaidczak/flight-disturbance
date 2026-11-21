locals {
  speed_consumer_name = "${var.project}-speed-consumer"
}

resource "aws_sns_topic" "events" {
  name                        = "${var.project}-events.fifo"
  fifo_topic                  = true
  content_based_deduplication = false
}

resource "aws_sqs_queue" "archive_events_dlq" {
  name                      = "${var.project}-archive-events-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "archive_events" {
  name                        = "${var.project}-archive-events.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 345600
  visibility_timeout_seconds  = 180
  sqs_managed_sse_enabled     = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.archive_events_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_sqs_queue" "speed_events_dlq" {
  name                      = "${var.project}-speed-events-dlq.fifo"
  fifo_queue                = true
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "speed_events" {
  name                        = "${var.project}-speed-events.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 345600
  visibility_timeout_seconds  = 180
  sqs_managed_sse_enabled     = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.speed_events_dlq.arn
    maxReceiveCount     = 5
  })
}

data "aws_iam_policy_document" "archive_events_from_sns" {
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.archive_events.arn]

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sns_topic.events.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "archive_events" {
  queue_url = aws_sqs_queue.archive_events.id
  policy    = data.aws_iam_policy_document.archive_events_from_sns.json
}

data "aws_iam_policy_document" "speed_events_from_sns" {
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.speed_events.arn]

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sns_topic.events.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "speed_events" {
  queue_url = aws_sqs_queue.speed_events.id
  policy    = data.aws_iam_policy_document.speed_events_from_sns.json
}

resource "aws_sns_topic_subscription" "archive_events" {
  topic_arn            = aws_sns_topic.events.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.archive_events.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.archive_events]
}

resource "aws_sns_topic_subscription" "speed_events" {
  topic_arn            = aws_sns_topic.events.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.speed_events.arn
  raw_message_delivery = true

  depends_on = [aws_sqs_queue_policy.speed_events]
}

resource "aws_dynamodb_table" "realtime" {
  name         = "${var.project}-realtime"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "entity_id"
  range_key    = "record_type"

  attribute {
    name = "entity_id"
    type = "S"
  }

  attribute {
    name = "record_type"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }
}

resource "aws_iam_role" "speed_consumer" {
  name               = "${local.speed_consumer_name}-role"
  assume_role_policy = data.aws_iam_policy_document.datalake_lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "speed_consumer_logs" {
  role       = aws_iam_role.speed_consumer.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "speed_consumer" {
  statement {
    sid       = "ReadSpeedQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.speed_events.arn]
  }

  statement {
    sid       = "ReadWriteRealtime"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.realtime.arn]
  }
}

resource "aws_iam_role_policy" "speed_consumer" {
  name   = "speed-consumer"
  role   = aws_iam_role.speed_consumer.id
  policy = data.aws_iam_policy_document.speed_consumer.json
}

resource "aws_cloudwatch_log_group" "speed_consumer" {
  name              = "/aws/lambda/${local.speed_consumer_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "speed_consumer" {
  function_name    = local.speed_consumer_name
  role             = aws_iam_role.speed_consumer.arn
  runtime          = var.lambda_runtime
  architectures    = ["x86_64"]
  handler          = "speed.handler.lambda_handler"
  filename         = "${path.module}/../build/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/../build/lambda.zip")
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      REALTIME_TABLE      = aws_dynamodb_table.realtime.name
      WEATHER_MAX_AGE_MIN = "30"
      NOAA_MAX_AGE_MIN    = "30"
      TTL_HOURS           = "24"
    }
  }

  depends_on = [aws_cloudwatch_log_group.speed_consumer, aws_iam_role_policy.speed_consumer]
}

resource "aws_lambda_event_source_mapping" "speed_consumer" {
  event_source_arn        = aws_sqs_queue.speed_events.arn
  function_name           = aws_lambda_function.speed_consumer.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]

  depends_on = [aws_iam_role_policy.speed_consumer]
}

output "events_topic_arn" {
  value = aws_sns_topic.events.arn
}

output "archive_events_queue_url" {
  value = aws_sqs_queue.archive_events.url
}

output "speed_events_queue_url" {
  value = aws_sqs_queue.speed_events.url
}

output "realtime_table_name" {
  value = aws_dynamodb_table.realtime.name
}

output "speed_consumer_function_name" {
  value = aws_lambda_function.speed_consumer.function_name
}
