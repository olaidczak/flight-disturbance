# Module: Pipeline Operational Monitoring and Alerting (`infra/monitoring.tf`)

## 1. Objective, Inputs & Outputs
Provides declarative AWS CloudWatch operational monitoring, alerting, and dashboarding for the ingestion, SNS/SQS FIFO fan-out, Bronze/Silver preprocessing, and Speed Layer pipeline in compliance with [AGENTS.md](../AGENTS.md), staying 100% within the **AWS Free Tier (\$0/month)**.

- **Inputs (Monitored Terraform Resources in `infra/`)**:
  - Speed SQS FIFO queue (`aws_sqs_queue.speed_events`) and Dead-Letter Queues (`aws_sqs_queue.speed_events_dlq`, `aws_sqs_queue.archive_events_dlq`, `aws_sqs_queue.silver_transform_dlq`)
  - **6 Lambda functions** registered in `local.monitored_lambdas`: `noaa_swpc`, `tomorrow_io`, `opensky`, `bronze_archiver`, `silver_transform`, `speed_consumer`
  - ECS Fargate service `seismic-ws` and CloudWatch log group `/ecs/flight-disruption-seismic-ws`
- **Outputs (`infra/monitoring.tf`, `infra/outputs.tf`)**:
  - **10 CloudWatch Metric Alarms** (standard resolution `300s`, utilizing 10/10 of the Free Tier alarm budget), all configured with `treat_missing_data = "notBreaching"` to prevent false alarms when schedules or containers are stopped
  - **1 CloudWatch Log Metric Filter** (`FlightDisruption/SeismicWs` -> `LogErrors`, pattern `?ERROR ?CRITICAL ?Traceback`)
  - **1 Consolidated CloudWatch Dashboard** (`flight-disruption-pipeline`, 7 operational widgets — including `archive_events_dlq` and `silver_transform_dlq` visibility without exceeding the 10-alarm Free Tier limit)
  - **Optional SNS Topic & Email Subscription** (created only when `-var="alarm_email=..."` is supplied)

## 2. Configured Alarms & Default Thresholds

| Alarm Name | Namespace / Metric (Dimension) | Statistic / Window | Default Threshold (Variable) |
|---|---|---|---|
| `sqs-dlq-messages` | `AWS/SQS` `ApproximateNumberOfMessagesVisible` (`speed-events-dlq.fifo`) | `Maximum`, `300s × 1` | `>= 1` (`alarm_dlq_messages_threshold`) |
| `sqs-queue-depth` | `AWS/SQS` `ApproximateNumberOfMessagesVisible` (`speed-events.fifo`) | `Maximum`, `300s × 2` | `> 500` (`alarm_sqs_depth_threshold`) |
| `sqs-oldest-message-age` | `AWS/SQS` `ApproximateAgeOfOldestMessage` (`speed-events.fifo`) | `Maximum`, `300s × 1` | `> 600s` (`alarm_sqs_age_threshold_seconds`) |
| `lambda-<key>-errors` (×6) | `AWS/Lambda` `Errors` (`noaa_swpc`, `tomorrow_io`, `opensky`, `bronze_archiver`, `silver_transform`, `speed_consumer`) | `Sum`, `300s × 1` | `>= 1` (`alarm_lambda_errors_threshold`) |
| `seismic-ws-log-errors` | `FlightDisruption/SeismicWs` `LogErrors` | `Sum`, `300s × 1` | `>= 3` (`alarm_seismic_errors_threshold`) |

> **Free Tier Budget Guard**: `infra/monitoring.tf` enforces `total_alarm_count <= 10` via a Terraform `check` block (`3 SQS + 6 Lambdas + 1 Seismic WS = 10/10`). `archive-events-dlq.fifo` and `silver-transform-dlq` are monitored visually on the dashboard to stay at \$0/month.

## 3. Test Documentation Table (Milestone 2 Report)

| Test ID | Objective | Steps | Expected Result | Actual Result (Evidence) |
|---|---|---|---|---|
| **T-MON-01** | Offline validation of Terraform syntax, Free Tier alarm budget (`10 <= 10`), `notBreaching` policy, and input validation | Inside `infra/`, run: `terraform fmt -check`, `terraform validate`, and `terraform test` | Configuration is valid and all 3 test runs in `infra/tests/monitoring.tftest.hcl` pass (10 alarms created, 0 SNS topics when email omitted, 1 SNS topic when email provided, invalid thresholds rejected). | `terraform validate`: Success. `terraform test`: 3 passed, 0 failed (`monitoring.tftest.hcl`). |
| **T-MON-02** | Verify zero false-positive alarms for idle/disabled components after deployment on AWS (`eu-north-1`) | Run `terraform apply`, then execute: `aws cloudwatch describe-alarms --alarm-name-prefix flight-disruption- --region eu-north-1 --query "MetricAlarms[].{Name:AlarmName,State:StateValue,Missing:TreatMissingData}" --output table` | All 10 alarms are provisioned in `eu-north-1` with `Missing = notBreaching` and remain in `OK` state when pollers or `seismic_ws` container (`desired_count = 0`) are idle. | *(Paste CLI table output / screenshot after `terraform apply`)* |
| **T-MON-03** | Verify DLQ alarm state transition (`OK` -> `ALARM` -> `OK`) and optional SNS notification delivery | Run `aws cloudwatch set-alarm-state --alarm-name flight-disruption-sqs-dlq-messages --state-value ALARM --state-reason "Milestone 2 verification test" --region eu-north-1`, wait 10s, then reset to `OK` | Alarm transitions to `ALARM` on the `Pipeline Alarm Status` dashboard widget (and sends an SNS email if `alarm_email` is subscribed), then returns to `OK` after reset. | *(Paste screenshot of alarm state transition on dashboard)* |
| **T-MON-04** | Verify consolidated operational CloudWatch Dashboard rendering | Open the console URL returned by `terraform output -raw cloudwatch_dashboard_url` | Dashboard `flight-disruption-pipeline` renders all 7 widgets: Alarm Status summary (10 alarms), SQS Speed Traffic & Backlog, SQS Oldest Message Age & all 3 DLQs (`speed_events_dlq`, `archive_events_dlq`, `silver_transform_dlq`), Lambda Invocations/Errors and Duration/Throttles (6 Lambdas), Seismic WS Log Errors, and Seismic WS ECS CPU/Memory. | *(Paste full CloudWatch Dashboard screenshot)* |
