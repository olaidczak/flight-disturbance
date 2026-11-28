variable "project" {
  type    = string
  default = "flight-disruption"
}

variable "region" {
  type    = string
  default = "eu-north-1"
}

variable "lambda_runtime" {
  type    = string
  default = "python3.12" # musi się zgadzać z wersją w scripts/build_lambda.sh i lokalnym venv
}

variable "log_retention_days" {
  type    = number
  default = 14
}

# Które pollery mają działać wg harmonogramu. Reszta dostaje schedule w stanie DISABLED.
# Dopisuj kolejne, gdy kod danego pollera jest gotowy i sekret ma wartość.
# opensky celowo wyłączony: dane OpenSky publikuje poller na Raspberry Pi (przez SNS),
# a chmurowy EventBridge Scheduler zużywałby tylko kredyty API (4000/dzień).
variable "enabled_pollers" {
  type    = list(string)
  default = ["noaa_swpc", "tomorrow_io"]
}

# Tag obrazu seismic_ws w ECR, który uruchamia serwis
variable "seismic_ws_image_tag" {
  type    = string
  default = "latest"
}

############ Operational Monitoring & Alerting (CloudWatch + Optional SNS) ############
variable "alarm_email" {
  description = "Optional operator email address for SNS CloudWatch alarm notifications (empty string disables email subscription)"
  type        = string
  default     = ""

  validation {
    condition     = var.alarm_email == "" || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alarm_email))
    error_message = "alarm_email must be an empty string or a valid email address."
  }
}

variable "alarm_dlq_messages_threshold" {
  description = "Minimum visible messages in DLQ required to trigger the sqs-dlq-messages alarm"
  type        = number
  default     = 1

  validation {
    condition     = var.alarm_dlq_messages_threshold >= 1
    error_message = "alarm_dlq_messages_threshold must be >= 1."
  }
}

variable "alarm_sqs_depth_threshold" {
  description = "Maximum allowed visible messages in the main SQS FIFO queue before triggering sqs-queue-depth alarm"
  type        = number
  default     = 500

  validation {
    condition     = var.alarm_sqs_depth_threshold >= 1
    error_message = "alarm_sqs_depth_threshold must be >= 1."
  }
}

variable "alarm_sqs_age_threshold_seconds" {
  description = "Maximum allowed age (in seconds) of the oldest message in the main SQS FIFO queue"
  type        = number
  default     = 600

  validation {
    condition     = var.alarm_sqs_age_threshold_seconds >= 60
    error_message = "alarm_sqs_age_threshold_seconds must be >= 60 seconds."
  }
}

variable "alarm_lambda_errors_threshold" {
  description = "Minimum execution errors in a 5-minute evaluation window to trigger a per-Lambda error alarm"
  type        = number
  default     = 1

  validation {
    condition     = var.alarm_lambda_errors_threshold >= 1
    error_message = "alarm_lambda_errors_threshold must be >= 1."
  }
}

variable "alarm_seismic_errors_threshold" {
  description = "Minimum error log matches in a 5-minute window to trigger the seismic-ws-log-errors alarm"
  type        = number
  default     = 3

  validation {
    condition     = var.alarm_seismic_errors_threshold >= 1
    error_message = "alarm_seismic_errors_threshold must be >= 1."
  }
}
