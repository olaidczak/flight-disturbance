# Flight Disturbance – Near-Real-Time Flight Disruption Analytics Platform

A cloud-native **AWS Lambda Architecture** platform for aviation operations analysts. It ingests and correlates live aircraft state vectors (**OpenSky Network**) with airport weather (**Tomorrow.io**), space weather (**NOAA SWPC**), and real-time seismic activity (**SeismicPortal EMSC**) to detect air traffic disruptions, build near-real-time contextual features, and prepare historical datasets for disruption risk modeling.

---

## Architecture Overview (Milestone 2 – Acquisition, Preprocessing & Monitoring)

```text
EventBridge Scheduler (1–15 min)        ECS Fargate (ARM64)
  ├─ opensky (REST / OAuth2)              └─ seismic_ws (WebSocket EMSC)
  ├─ tomorrow_io (REST / 2 API keys)
  └─ noaa_swpc (HTTP JSON: wind/mag/kp)
                 │                                  │
                 └───────────────┬──────────────────┘
                                 ▼
                  Amazon SNS FIFO (events.fifo)
                      raw_message_delivery=true
                  ┌──────────────┴──────────────┐
                  ▼ (Batch / Data Lake)         ▼ (Speed Layer)
       SQS FIFO: archive-events.fifo     SQS FIFO: speed-events.fifo
            (+ archive-events-dlq)            (+ speed-events-dlq)
                  │                             │
                  ▼                             ▼
       Lambda: bronze_archiver          Lambda: speed_consumer
       (If-None-Match: * write-once)    (to_silver + light_features +
                  │                      nearest airport/NOAA join)
                  ▼                             │
         S3 Data Lake: bronze/                  ▼
                  │ (ObjectCreated)     DynamoDB: flight-disruption-realtime
                  ▼                     (LATEST views, 24h TTL, version_time)
       Lambda: silver_transform
       (clean, filter, LATEST_WINS)
                  │
                  ▼
         S3 Data Lake: silver/
```

- **Region & Stack**: AWS `eu-north-1` (Stockholm), Python `3.12`, Terraform `>= 1.10` (`hashicorp/aws >= 5.0`), remote state in S3 bucket `flight-disruption-tfstate-592964949672` (`use_lockfile = true`).
- **Observability**: 10 CloudWatch Alarms (10/10 AWS Free Tier), `seismic_ws` log metric filter, consolidated 7-widget CloudWatch Dashboard (`flight-disruption-pipeline`), and optional SNS email alerts.
- **Planned (Milestone 3)**: Gold spatiotemporal batch layer, ML disruption risk model & explainability (`src/ml/`), custom EC2 query service (`src/query_service/`), and Streamlit operations dashboard (`src/dashboard/`).

---

## Repository Structure

```text
src/
├── common/         # Shared Record schema (schema.py), SNS/SQS publisher (sqs.py), Secrets Manager (secrets.py)
├── ingestion/      # Scheduled REST pollers: opensky.py, tomorrow_io.py, noaa_swpc.py
├── seismic_ws/     # Persistent WebSocket client (main.py) & ARM64 Dockerfile for ECS Fargate
├── processing/     # Data Lake Lambdas: archiver.py (Bronze), silver_handler.py & silver_transforms.py (Silver)
├── speed/          # Speed Layer Lambda: handler.py & features.py (light features + DynamoDB realtime store)
├── ml/             # Planned (Milestone 3): ML training & inference
├── query_service/  # Planned (Milestone 3): Custom EC2 query service
└── dashboard/      # Planned (Milestone 3): Streamlit dashboard
infra/              # Terraform IaC (S3, SNS/SQS FIFO, Lambda, ECS, DynamoDB, IAM, Secrets, CloudWatch Monitoring)
├── tests/          # Offline Terraform unit tests (monitoring.tftest.hcl)
tests/              # Offline pytest suites (test_pollers_bronze.py, test_lake.py, test_speed.py) & API samples
docs/               # Event schema (event-schema.md), module specs & Milestone test tables (monitoring.md, speed-layer.md)
scripts/            # Lambda packaging script (build_lambda.sh)
```

---

## Running Automated Tests Locally (Offline, Zero AWS Cost)

### 1. Python Unit & Transformation Quality Tests (`pytest`)
```bash
python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate.ps1  |  Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt boto3
PYTHONPATH=src pytest -q
```

### 2. Terraform Infrastructure Validation & Unit Tests
```bash
terraform -chdir=infra fmt -check
terraform -chdir=infra validate
terraform -chdir=infra test
```

---

## Deployment (`eu-north-1`)

1. **Package Poller & Speed Layer Lambdas (`build/lambda.zip`)**:
   ```bash
   bash scripts/build_lambda.sh
   ```
   *(Bronze/Silver Lambdas are packaged automatically by Terraform into `build/datalake_layers.zip`.)*
2. **Apply Terraform Infrastructure**:
   ```bash
   cd infra
   terraform init
   terraform plan
   terraform apply
   ```
   *(Optional email alerts: `terraform apply -var="alarm_email=your@email.com"`.)*
3. **Build & Push `seismic_ws` Container Image (ARM64 for ECS Fargate)**:
   ```bash
   docker buildx build --platform linux/arm64 -f src/seismic_ws/Dockerfile -t <seismic_ws_ecr_url>:latest --push src
   ```

---

## Documentation & Workflow

- **[AGENTS.md](AGENTS.md)** – Project constitution, architecture constraints, rate limits, and AI agent guidelines.
- **[docs/event-schema.md](docs/event-schema.md)** – Shared `Record` contract (Bronze), Silver transformations, and DynamoDB Speed Layer schema.
- **[docs/monitoring.md](docs/monitoring.md)** & **[docs/speed-layer.md](docs/speed-layer.md)** – Module specifications and Milestone 2 test documentation tables.
- **[README_SPECKIT.md](README_SPECKIT.md)** – Minimalist 1-file Spec-Driven workflow guide for team contributors.
