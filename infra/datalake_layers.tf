# Warstwy bronze i silver w istniejącym data lake (aws_s3_bucket.datalake z s3.tf): dwie Lambdy.
#
#   SQS FIFO --(event source mapping)--> Lambda bronze_archiver --> s3://<datalake>/bronze/...
#   s3://<datalake>/bronze/*.json --(ObjectCreated)--> Lambda silver_transform --> s3://<datalake>/silver/...
#
# Plik niczego nie zmienia w samym buckecie ani w s3.tf: tylko czyta aws_s3_bucket.datalake
# (arn, id, nazwa) i dokłada na nim powiadomienie ObjectCreated dla prefiksu bronze/.
#
# Czego plik oczekuje od reszty infra/:
#   - var.project i data.aws_caller_identity.current (już używane w s3.tf, tu ich NIE deklarujemy),
#   - kolejka aws_sqs_queue.events z sqs.tf (visibility timeout 180 s = 6 x timeout archivera 30 s;
#     jeśli zwiększycie timeout archivera, zwiększcie też visibility timeout kolejki),
#   - bucket może mieć tylko JEDEN zasób aws_s3_bucket_notification. Sprawdźcie:
#       grep -rn aws_s3_bucket_notification infra/
#     Jeśli już jest, dołóżcie do niego blok lambda_function z dołu pliku zamiast drugiego zasobu.
#   - jeśli kolejka lub bucket używają własnego klucza KMS, dodajcie kms:Decrypt /
#     kms:GenerateDataKey do ról poniżej.
#
# Pakiet Lambd: katalog src/ bez ingestion/, dashboard/, ml/, query_service/, seismic_ws/
# (zostaje common/ i processing/). boto3 jest w runtime Lambdy.

locals {
  bronze_archiver_name   = "${var.project}-bronze-archiver"
  silver_transform_name  = "${var.project}-silver-transform"
  layers_source_dir      = "${path.module}/../src"
  layers_log_retention   = 14
  layers_excluded_dirs   = ["ingestion", "dashboard", "ml", "query_service", "seismic_ws", "__pycache__"]
}

data "archive_file" "datalake_layers" {
  type        = "zip"
  source_dir  = local.layers_source_dir
  output_path = "${path.module}/../build/datalake_layers.zip"
  excludes    = local.layers_excluded_dirs
}

data "aws_iam_policy_document" "datalake_lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ------------------------------------------------------------ bronze_archiver (SQS -> bronze/)
resource "aws_iam_role" "bronze_archiver" {
  name               = "${local.bronze_archiver_name}-role"
  assume_role_policy = data.aws_iam_policy_document.datalake_lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "bronze_archiver_logs" {
  role       = aws_iam_role.bronze_archiver.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "bronze_archiver" {
  statement {
    sid       = "ReadQueue"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.events.arn]
  }
  statement {
    sid       = "WriteBronze"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/bronze/*"]
  }
}

resource "aws_iam_role_policy" "bronze_archiver" {
  name   = "bronze-archiver"
  role   = aws_iam_role.bronze_archiver.id
  policy = data.aws_iam_policy_document.bronze_archiver.json
}

resource "aws_cloudwatch_log_group" "bronze_archiver" {
  name              = "/aws/lambda/${local.bronze_archiver_name}"
  retention_in_days = local.layers_log_retention
}

resource "aws_lambda_function" "bronze_archiver" {
  function_name    = local.bronze_archiver_name
  role             = aws_iam_role.bronze_archiver.arn
  runtime          = "python3.12"
  handler          = "processing.archiver.lambda_handler"
  filename         = data.archive_file.datalake_layers.output_path
  source_code_hash = data.archive_file.datalake_layers.output_base64sha256
  timeout          = 30 # visibility timeout kolejki events (180 s) / 6
  memory_size      = 256

  environment {
    variables = {
      LAKE_BUCKET       = aws_s3_bucket.datalake.bucket
      BRONZE_WRITE_ONCE = "true"
    }
  }

  depends_on = [aws_cloudwatch_log_group.bronze_archiver, aws_iam_role_policy_attachment.bronze_archiver_logs]
}

resource "aws_lambda_event_source_mapping" "bronze_archiver_sqs" {
  event_source_arn = aws_sqs_queue.events.arn
  function_name    = aws_lambda_function.bronze_archiver.arn
  batch_size       = 10
  # archiver zwraca batchItemFailures: ponawiane są tylko wiadomości, które się nie udały
  function_response_types = ["ReportBatchItemFailures"]

  depends_on = [aws_iam_role_policy.bronze_archiver]
}

# ------------------------------------------------------- silver_transform (bronze/ -> silver/)
resource "aws_sqs_queue" "silver_transform_dlq" {
  name                      = "${local.silver_transform_name}-dlq"
  message_retention_seconds = 1209600 # 14 dni
  sqs_managed_sse_enabled   = true
}

resource "aws_iam_role" "silver_transform" {
  name               = "${local.silver_transform_name}-role"
  assume_role_policy = data.aws_iam_policy_document.datalake_lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "silver_transform_logs" {
  role       = aws_iam_role.silver_transform.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "silver_transform" {
  statement {
    sid       = "ReadBronze"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/bronze/*"]
  }
  statement {
    sid       = "WriteSilver"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.datalake.arn}/silver/*"]
  }
  statement {
    sid       = "FailedEventsToDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.silver_transform_dlq.arn]
  }
}

resource "aws_iam_role_policy" "silver_transform" {
  name   = "silver-transform"
  role   = aws_iam_role.silver_transform.id
  policy = data.aws_iam_policy_document.silver_transform.json
}

resource "aws_cloudwatch_log_group" "silver_transform" {
  name              = "/aws/lambda/${local.silver_transform_name}"
  retention_in_days = local.layers_log_retention
}

resource "aws_lambda_function" "silver_transform" {
  function_name    = local.silver_transform_name
  role             = aws_iam_role.silver_transform.arn
  runtime          = "python3.12"
  handler          = "processing.silver_handler.lambda_handler"
  filename         = data.archive_file.datalake_layers.output_path
  source_code_hash = data.archive_file.datalake_layers.output_base64sha256
  timeout          = 30
  memory_size      = 256

  depends_on = [aws_cloudwatch_log_group.silver_transform, aws_iam_role_policy_attachment.silver_transform_logs]
}

# S3 wywołuje Lambdę asynchronicznie: po 2 nieudanych próbach zdarzenie ląduje w DLQ.
# Jeden zapis do bronze = jedno wywołanie, więc przy gęstym pollingu OpenSky pojawia się
# wiele równoległych wywołań (limit współbieżności konta, domyślnie 1000, bywa 10 na nowych
# kontach; zdarzenia ograniczone limitem są ponawiane automatycznie).
resource "aws_lambda_function_event_invoke_config" "silver_transform" {
  function_name                = aws_lambda_function.silver_transform.function_name
  maximum_retry_attempts       = 2
  maximum_event_age_in_seconds = 3600

  destination_config {
    on_failure {
      destination = aws_sqs_queue.silver_transform_dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.silver_transform]
}

resource "aws_lambda_permission" "silver_transform_from_s3" {
  statement_id   = "AllowInvokeFromDatalakeBucket"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.silver_transform.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.datalake.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "datalake_bronze_created" {
  bucket = aws_s3_bucket.datalake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.silver_transform.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "bronze/" # silver/ nie może wyzwalać samego siebie
    filter_suffix       = ".json"
  }

  depends_on = [aws_lambda_permission.silver_transform_from_s3]
}

output "bronze_archiver_function_name" {
  value = aws_lambda_function.bronze_archiver.function_name
}

output "silver_transform_function_name" {
  value = aws_lambda_function.silver_transform.function_name
}

output "silver_transform_dlq_url" {
  value = aws_sqs_queue.silver_transform_dlq.url
}
