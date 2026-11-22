# Module: Raspberry Pi OpenSky Speed Path

## 1. Objective, Inputs & Outputs

The Raspberry Pi OpenSky poller now publishes aircraft records to the existing
SNS FIFO topic. SNS copies every record to the archive and speed SQS queues, so
the same input updates both S3 and the real-time DynamoDB table.

- **Input**: OpenSky records produced every 60 seconds by `src/ingestion/opensky.py`.
- **Configuration**: `AWS_REGION`, `TOPIC_ARN`, and `OPENSKY_SECRET_ARN`.
- **Output**: messages in the archive and speed SQS FIFO queues.
- **Cost**: no new AWS resources and no fixed-hourly cost.
- **Rate limit**: polling remains once per minute.
- **Idempotency**: the existing deterministic `event_id` and FIFO deduplication are unchanged.

## 2. Files Created / Modified

- `scripts/run_poller_local.sh` - provides the existing SNS FIFO topic ARN.
- `tests/test_common_sqs.py` - verifies SNS is preferred over legacy SQS.
- `docs/raspberry-pi-opensky-speed-path.md` - rollout steps and test evidence.
- `docs/raspberry-pi-iam.md` - least-privilege IAM setup.

## 3. Raspberry Pi Rollout

1. Merge this branch into the branch used on Raspberry Pi.
2. Stop the running OpenSky poller.
3. Open the repository on Raspberry Pi and run `git pull`.
4. Confirm that `.venv` exists and contains `boto3` and `requests`.
5. Apply the permissions from `docs/raspberry-pi-iam.md`.
6. Run `aws sts get-caller-identity` and confirm the expected AWS identity.
7. From the repository root run:
   `bash scripts/run_poller_local.sh opensky --interval 60`.
8. Check the log for a positive `sent` value and no `AccessDenied` error.
9. If systemd manages the poller, restart that service instead of step 7.

Do not save OpenSky secrets or AWS access keys in Git. The poller reads the
OpenSky credentials from AWS Secrets Manager.

## 4. Expected Data Flow

`OpenSky -> Raspberry Pi -> SNS -> archive SQS -> Bronze -> Silver`

`OpenSky -> Raspberry Pi -> SNS -> speed SQS -> speed_consumer -> DynamoDB`

## 5. Test Documentation Table

| Test ID | Objective | Steps | Expected Result | Actual Result |
|---|---|---|---|---|
| **T-OSP-01** | Verify SNS selection | Run `PYTHONPATH=src pytest -q tests/test_common_sqs.py` | SNS receives the record and legacy SQS is not called. | Passed: `1 passed`. |
| **T-OSP-02** | Verify Python regression tests | Run `PYTHONPATH=src pytest -q` on Python 3.12. | All Python tests pass. | Passed: `54 passed`. |
| **T-OSP-03** | Verify infrastructure | Run `terraform -chdir=infra validate` and `terraform -chdir=infra test`. | Configuration is valid and all Terraform tests pass. | Passed: valid; `3 passed, 0 failed`. |
| **T-OSP-04** | Verify live fan-out | Start one poll and inspect S3 Bronze/Silver and DynamoDB. | One OpenSky event reaches both paths without DLQ errors. | Not run: requires Raspberry Pi and authenticated AWS access. |
