# Module: Pipeline Operational Monitoring and Alerting (`infra/monitoring.tf`)

## 1. Objective, Inputs & Outputs
Provides declarative AWS CloudWatch operational monitoring, alerting, and dashboarding for the ingestion, buffering, and Bronze/Silver preprocessing pipeline in compliance with [AGENTS.md](../AGENTS.md), staying 100% within the **AWS Free Tier (\$0/month)**.

- **Inputs (Monitored Terraform Resources in `infra/`)**:
  - Main SQS FIFO queue (`aws_sqs_queue.events`) and Dead-Letter Queues (`aws_sqs_queue.events_dlq`, `aws_sqs_queue.silver_transform_dlq`)
  - 5 Lambda functions registered in `local.monitored_lambdas`: `noaa_swpc`, `tomorrow_io`, `opensky`, `bronze_archiver`, `silver_transform`
  - ECS Fargate service `seismic-ws` and CloudWatch log group `/ecs/flight-disruption-seismic-ws`
- **Outputs (`infra/monitoring.tf`, `infra/outputs.tf`)**:
  - **9 CloudWatch Metric Alarms** (standard resolution `300s`, within the 10-alarm Free Tier limit), all configured with `treat_missing_data = "notBreaching"` to prevent false alarms when schedules or containers are stopped
  - **1 CloudWatch Log Metric Filter** (`FlightDisruption/SeismicWs` -> `LogErrors`, pattern `?ERROR ?CRITICAL ?Traceback`)
  - **1 Consolidated CloudWatch Dashboard** (`flight-disruption-pipeline`, 7 operational widgets)
  - **Optional SNS Topic & Email Subscription** (created only when `-var="alarm_email=..."` is supplied)

## 2. Configured Alarms & Default Thresholds

| Alarm Name | Namespace / Metric (Dimension) | Statistic / Window | Default Threshold (Variable) |
|---|---|---|---|
| `sqs-dlq-messages` | `AWS/SQS` `ApproximateNumberOfMessagesVisible` (`events-dlq.fifo`) | `Maximum`, `300s × 1` | `>= 1` (`alarm_dlq_messages_threshold`) |
| `sqs-queue-depth` | `AWS/SQS` `ApproximateNumberOfMessagesVisible` (`events.fifo`) | `Maximum`, `300s × 2` | `> 500` (`alarm_sqs_depth_threshold`) |
| `sqs-oldest-message-age` | `AWS/SQS` `ApproximateAgeOfOldestMessage` (`events.fifo`) | `Maximum`, `300s × 1` | `> 600s` (`alarm_sqs_age_threshold_seconds`) |
| `lambda-<key>-errors` (×5) | `AWS/Lambda` `Errors` (`noaa_swpc`, `tomorrow_io`, `opensky`, `bronze_archiver`, `silver_transform`) | `Sum`, `300s × 1` | `>= 1` (`alarm_lambda_errors_threshold`) |
| `seismic-ws-log-errors` | `FlightDisruption/SeismicWs` `LogErrors` | `Sum`, `300s × 1` | `>= 3` (`alarm_seismic_errors_threshold`) |

> **Extensibility**: To onboard an additional Lambda function to full error alarming and dashboard widgets, add a single key-value entry to `local.monitored_lambdas` in [infra/monitoring.tf](../infra/monitoring.tf) (1 free alarm slot remains under the 10-alarm Free Tier budget).

## 3. Test Documentation Table (Milestone 2 Report)

| Test ID | Objective | Steps | Expected Result | Actual Result (Evidence) |
|---|---|---|---|---|
| **T-MON-01** | Offline validation of Terraform syntax, Free Tier alarm budget (`<= 10`), `notBreaching` policy, and input validation | Inside `infra/`, run: `terraform fmt -check`, `terraform validate`, and `terraform test` | Configuration is valid and all 3 test runs in `infra/tests/monitoring.tftest.hcl` pass (9 alarms created, 0 SNS topics when email omitted, 1 SNS topic when email provided, invalid thresholds rejected). | `terraform validate`: Success. `terraform test`: 3 passed, 0 failed (`monitoring.tftest.hcl`). |
| **T-MON-02** | Verify zero false-positive alarms for idle/disabled components after deployment on AWS (`eu-north-1`) | Run `terraform apply`, then execute: `aws cloudwatch describe-alarms --alarm-name-prefix flight-disruption- --region eu-north-1 --query "MetricAlarms[].{Name:AlarmName,State:StateValue,Missing:TreatMissingData}" --output table` | All 9 alarms are provisioned in `eu-north-1` with `Missing = notBreaching` and remain in `OK` state when pollers or `seismic_ws` container (`desired_count = 0`) are idle. | *(Paste CLI table output / screenshot after `terraform apply`)* |
| **T-MON-03** | Verify DLQ alarm state transition (`OK` -> `ALARM` -> `OK`) and optional SNS notification delivery | Run `aws cloudwatch set-alarm-state --alarm-name flight-disruption-sqs-dlq-messages --state-value ALARM --state-reason "Milestone 2 verification test" --region eu-north-1`, wait 10s, then reset to `OK` | Alarm transitions to `ALARM` on the `Pipeline Alarm Status` dashboard widget (and sends an SNS email if `alarm_email` is subscribed), then returns to `OK` after reset. | *(Paste screenshot of alarm state transition on dashboard)* |
| **T-MON-04** | Verify consolidated operational CloudWatch Dashboard rendering | Open the console URL returned by `terraform output -raw cloudwatch_dashboard_url` | Dashboard `flight-disruption-pipeline` renders all 7 widgets: Alarm Status summary, SQS Traffic & Backlog, SQS Oldest Message Age & both DLQs, Lambda Invocations/Errors, Lambda Duration/Throttles, Seismic WS Log Errors, and Seismic WS ECS CPU/Memory. | *(Paste full CloudWatch Dashboard screenshot)* |
