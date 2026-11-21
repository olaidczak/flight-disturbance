# Module: <Module Name> (`<primary files in src/ or infra/>`)

## 1. Objective, Inputs & Outputs
Brief description (2–4 sentences) of what this module does within the system architecture ([AGENTS.md](../AGENTS.md)).

- **Inputs**: Data sources, SQS queues, S3 prefixes, or environment variables consumed by this module.
- **Outputs**: Produced `Record` events ([src/common/schema.py](../src/common/schema.py)), S3 objects, or provisioned AWS resources.
- **Rate Limits & Cost Boundaries (AWS Free Tier)**: Polling frequency, idempotency guarantees, and confirmation of \$0/month Free Tier compliance.

## 2. Files Created / Modified
- `src/...` – primary application or handler logic
- `infra/...` – (optional) Terraform infrastructure definitions
- `tests/...` – automated unit tests (`pytest` or `terraform test`)

## 3. Test Documentation Table (Ready for Milestone Report)

| Test ID | Objective | Steps | Expected Result | Actual Result (Log / Screenshot Evidence) |
|---|---|---|---|---|
| **T-XXX-01** | Unit test of data transformation / validation (offline) | Run `PYTHONPATH=src pytest -q tests/test_<module>.py` | All test assertions pass; valid schema output and error handling verified. | *(paste pytest output)* |
| **T-XXX-02** | Live integration verification on AWS (`eu-north-1`) | Trigger Lambda / service and inspect CloudWatch Logs / S3 objects | Events processed idempotently into target layer with 0 unhandled DLQ errors. | *(paste CloudWatch log snippet or screenshot reference)* |
