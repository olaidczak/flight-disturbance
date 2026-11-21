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

output "cloudwatch_dashboard_name" {
  value = aws_cloudwatch_dashboard.pipeline.dashboard_name
}

output "cloudwatch_dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards/dashboard/${aws_cloudwatch_dashboard.pipeline.dashboard_name}"
}

output "cloudwatch_alarm_names" {
  value = local.all_alarm_names
}

output "alarms_sns_topic_arn" {
  value = var.alarm_email != "" ? aws_sns_topic.alarms[0].arn : null
}
