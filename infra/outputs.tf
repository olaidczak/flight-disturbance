output "queue_url" {
  value = aws_sqs_queue.events.url
}

output "queue_arn" {
  value = aws_sqs_queue.events.arn
}

output "dlq_url" {
  value = aws_sqs_queue.events_dlq.url
}

output "datalake_bucket" {
  value = aws_s3_bucket.datalake.bucket
}

output "lambda_role_arn" {
  value = aws_iam_role.lambda.arn
}

output "secret_names" {
  value = {
    opensky     = aws_secretsmanager_secret.opensky.name
    tomorrow_io = aws_secretsmanager_secret.tomorrow_io.name
  }
}

output "seismic_ws_ecr_url" {
  value = aws_ecr_repository.seismic_ws.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.main.name
}

output "realtime_table" {
  value = aws_dynamodb_table.realtime.name
}
