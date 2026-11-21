# Flight Disturbance – Project Constitution & AI Agent Rules (`AGENTS.md`)

> This file is automatically loaded by AI coding agents (Codex, Antigravity, Claude Code, Cursor) at session startup. It serves as the single source of truth for project architecture, data contracts, cost boundaries, and the minimalist Spec-Driven workflow.

---

## 1. Project Goal

A near-real-time analytics platform for aviation operations analysts. It correlates live aircraft state vectors with weather, seismic, and space-weather events to detect air traffic disruptions, estimate short-term disruption risk, and explain primary contributing factors based on spatiotemporal correlation (event time + geographic proximity). Current phase (**Project Milestone 2**): data ingestion, preprocessing (`bronze` -> `silver`), and operational monitoring.

---

## 2. Target Architecture (AWS Lambda Architecture)

- **Ingestion (`src/ingestion/`, `src/seismic_ws/`)**:
  - EventBridge Scheduler triggers REST polling Lambda functions (`opensky`, `tomorrow_io`, `noaa_swpc`).
  - ECS Fargate runs a persistent WebSocket client (`seismic_ws` -> SeismicPortal EMSC).
- **Buffering (`infra/sqs.tf`)**:
  - Amazon SQS FIFO (`flight-disruption-events.fifo`) + Dead-Letter Queue (`flight-disruption-events-dlq.fifo`).
  - Every event carries a deterministic `event_id`, `MessageGroupId` (`Record.message_group_id`), and `MessageDeduplicationId` (SHA-256 of `event_id`).
- **Speed Layer & Data Lake (`src/processing/`, `infra/datalake_layers.tf`, `infra/s3.tf`)**:
  - **Bronze (`archiver.py`)**: Lambda `bronze_archiver` consumes SQS FIFO batches (`ReportBatchItemFailures`), validates the `Record` envelope, and writes immutable raw JSON objects to `s3://<bucket>/bronze/source=<source>/date=<YYYY-MM-DD>/<safe_id>.json` using conditional writes (`If-None-Match: *`).
  - **Silver (`silver_handler.py`, `silver_transforms.py`)**: S3 `ObjectCreated` events on prefix `bronze/` trigger Lambda `silver_transform`, which cleans, filters (e.g., drops inactive NOAA satellites), normalizes field names/types, and writes cleaned documents to `s3://<bucket>/silver/source=<source>/date=<YYYY-MM-DD>/<safe_id>.json` (with a dedicated DLQ for failed async invocations).
  - **Gold / Batch Layer (upcoming)**: joined spatiotemporal features, labels, and aggregations (Glue / EMR Serverless / Lambda).
- **Replay**: A custom producer reads archived raw events from S3 (`bronze/`) and republishes them to SQS FIFO preserving original `event_time` timestamps.
- **Operational Monitoring (`infra/monitoring.tf`)**: CloudWatch Alarms (queue depth, oldest message age, DLQ activity, per-Lambda errors, `seismic_ws` log errors), a consolidated CloudWatch Dashboard (`flight-disruption-pipeline`), and optional SNS email alerts.
- **Serving (later stages)**: Step Functions + SageMaker, DynamoDB (real-time views), Athena / custom query service on EC2, API Gateway + Lambda, Streamlit on ECS Fargate.

---

## 3. Data Sources & Rate Limits (Pollers MUST Respect These)

| Source (`source`) | Access Method | Rate Limits / Schedule | `event_id` Format |
|---|---|---|---|
| `opensky` | REST (OAuth2) | 4,000 credits/day, Poland bbox = 2 credits/call, scheduled every 1 min | `opensky:{icao24}:{last_contact}` |
| `tomorrow_io` | REST (API key) | 500 req/day, 25/h, 3/s per key (2 rotated keys, 5 airports every 15 min) | `tomorrow_io:{airport_icao}:{time}` |
| `seismic` | WebSocket | No rate limit, irregular stream, hundreds of events/day | `seismic:{unid}:{lastupdate}` |
| `noaa_swpc` | HTTP JSON | No rate limit; solar wind (`wind`) and magnetic field (`mag`) 1-min feed, `kp` 3-hour feed | `noaa_swpc:{wind\|mag}:{sat}:{time}` / `noaa_swpc:kp:{time}:{Kp}` |

---

## 4. Record Contract & Processing Rules

1. **Single Source of Truth for Schema**: [`src/common/schema.py`](src/common/schema.py) (`Record` dataclass) and [`docs/event-schema.md`](docs/event-schema.md). Do not modify `schema.py` without team agreement.
2. Every record contains `event_id`, `source`, `event_time` (ISO 8601 UTC), `ingestion_time` (ISO 8601 UTC), `location` (`{"lat": ..., "lon": ...}` or `None`), and `payload` (raw data in Bronze).
3. Delivery is at-least-once -> **all processing must be idempotent**: deterministic S3 object keys (`Record.s3_key_bronze` and `Record.s3_key_silver`).
4. Raw data in `bronze/` is strictly immutable; all cleaning, filtering, and type coercion occur during the `bronze/` -> `silver/` transition.

---

## 5. Security, Cost Control (AWS Free Tier), and Quality

- **Secrets**: API keys and credentials must reside only in AWS Secrets Manager or environment variables. **Never** commit keys or private email addresses to code, Git, `terraform.tfstate`, or AI prompts.
- **IAM**: Enforce least-privilege access policies.
- **Cost Control (AWS Free Tier & Student Credits)**: Never provision fixed-hourly paid resources without explicit team approval (e.g., no NAT Gateways, no \$1/mo Customer Managed KMS keys, no more than 10 CloudWatch alarms). Keep `seismic_ws` ECS Fargate `desired_count = 0` when not actively testing.
- **Testing & Documentation**: Every module must include automated tests (`pytest` in `tests/` for Python, `terraform test` in `infra/tests/` for IaC) and a structured test documentation table (*Test ID | Objective | Steps | Expected Result | Actual Result*) ready for the Milestone report.

---

## 6. Minimalist Spec-Driven Process (How We Work with AI)

We do **not** use multi-stage generators that create 7 markdown files per feature. Instead, every new module follows a **1-file workflow**:

1. **Before implementation**: Create or update **one concise file `docs/<module-name>.md` in English** (40–60 lines max, following [`docs/_module-template.md`](docs/_module-template.md)) covering:
   - Goal, inputs, outputs, and AWS Free Tier / rate-limit compliance,
   - List of files to create or modify in `src/`, `infra/`, and `tests/`,
   - **Test Documentation Table** (ready to copy-paste directly into the English Milestone 2 / 3 report).
2. **After human review**: Implement the code and unit tests following existing conventions in `src/`, `infra/`, and `tests/`, then run verification (`PYTHONPATH=src pytest -q` and `terraform -chdir=infra test`).
3. **Across future project phases (Gold layer, ML, Custom EC2 Query Service, Dashboard)**:
   - At the start of each new phase, update `AGENTS.md` with finalized architectural decisions and append new cross-module data/API contracts (e.g., Gold feature schema or query API endpoints) to [`docs/event-schema.md`](docs/event-schema.md).
   - Keep all previous `docs/<module-name>.md` files in `docs/` as cumulative architecture and test documentation for the final report.
