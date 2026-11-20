  locals {
    # Jedna definicja na wszystkie pollery. Nowy poller = nowy wpis w mapie.
    # Limity: OpenSky ~4000 kredytów/dzień, Tomorrow.io 500 zapytań/dzień na plan darmowy.
    # EventBridge Scheduler nie schodzi poniżej 1 minuty.
    pollers = {
      noaa_swpc = {
        schedule = "rate(5 minutes)"
        secret   = ""
      }
      tomorrow_io = {
        schedule = "rate(15 minutes)" #
        secret   = aws_secretsmanager_secret.tomorrow_io.name
      }
      opensky = {
        schedule = "rate(1 minute)" # 1440 kredytów/dzień na jeden bounding box
        secret   = aws_secretsmanager_secret.opensky.name
      }
    }
  }

  resource "aws_cloudwatch_log_group" "poller" {
    for_each          = local.pollers
    name              = "/aws/lambda/${var.project}-${each.key}"
    retention_in_days = var.log_retention_days
  }

  # Paczka powstaje przez scripts/build_lambda.sh -> build/lambda.zip
  resource "aws_lambda_function" "poller" {
    for_each = local.pollers

    function_name    = "${var.project}-${each.key}"
    role             = aws_iam_role.lambda.arn
    runtime          = var.lambda_runtime
    architectures    = ["x86_64"]
    handler          = "ingestion.${each.key}.lambda_handler" # src/ingestion/<nazwa>.py
    filename         = "${path.module}/../build/lambda.zip"
    source_code_hash = filebase64sha256("${path.module}/../build/lambda.zip")
    timeout          = 60
    memory_size      = 256

    environment {
      variables = {
        QUEUE_URL   = aws_sqs_queue.events.url
        BUCKET      = aws_s3_bucket.datalake.bucket
        SECRET_NAME = each.value.secret
      }
    }

    depends_on = [aws_cloudwatch_log_group.poller, aws_iam_role_policy.lambda_access]
  }

  # EventBridge Scheduler: co ile wywołać każdego pollera
  resource "aws_scheduler_schedule" "poller" {
    for_each = local.pollers

    name                = "${var.project}-${each.key}"
    schedule_expression = each.value.schedule
    state               = contains(var.enabled_pollers, each.key) ? "ENABLED" : "DISABLED"

    flexible_time_window {
      mode = "OFF"
    }

    target {
      arn      = aws_lambda_function.poller[each.key].arn
      role_arn = aws_iam_role.scheduler.arn
      retry_policy {
        maximum_retry_attempts = 0 # kolejne wywołanie i tak przyjdzie za chwilę
      }
    }
  }
