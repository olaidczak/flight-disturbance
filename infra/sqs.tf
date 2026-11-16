# Kolejka martwych wiadomości (dla FIFO też musi być FIFO)
resource "aws_sqs_queue" "events_dlq" {
  name                       = "${var.project}-events-dlq.fifo"
  fifo_queue                 = true
  message_retention_seconds  = 1209600 # 14 dni
  sqs_managed_sse_enabled    = true
}

# Główna kolejka zdarzeń. Deduplication id podajemy sami (Record.deduplication_id),
# więc content_based_deduplication zostaje wyłączone.
resource "aws_sqs_queue" "events" {
  name                        = "${var.project}-events.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  message_retention_seconds   = 345600 # 4 dni
  visibility_timeout_seconds  = 180    # >= 6x timeout konsumenta (Lambda), dostosuj później
  sqs_managed_sse_enabled     = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.events_dlq.arn
    maxReceiveCount     = 5
  })
}
