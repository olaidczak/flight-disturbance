# Module: Batch Parquet Pipeline (`src/processing/archiver.py`, `src/batch/batch_job.py`, `infra/glue_batch.tf`)

## 1. Objective, Inputs & Outputs
Replaces one-JSON-object-per-record storage (≈230k objects/day, one Lambda invocation per bronze object) with a nightly AWS Glue (Spark) job that compacts raw events into Parquet. See [AGENTS.md](../AGENTS.md) (Batch Layer) and [docs/event-schema.md](event-schema.md).

- **Inputs**: SQS FIFO `archive-events` (via `bronze_archiver`), `raw/ingest_date=D/source=X/<hash>.jsonl`, legacy `bronze/*.json` (one-off backfill only).
- **Outputs**: `lake/bronze/` (Parquet, partitions `ingest_date`, `source`; deduplicated by `event_id`), `lake/silver/<dataset>/` (Parquet, partition `ingest_date`, explicit schemas in `batch_schemas.py`; datasets: `opensky`, `tomorrow_io`, `seismic`, `noaa_swpc_wind|mag|kp`), done markers `lake/_done/ingest_date=D`.
- **Flow**: `bronze_archiver` writes one JSONL batch per source per SQS batch to `raw/` (S3 lifecycle: 3 days). Glue job `flight-disruption-batch` (02:00 UTC) processes every finished day without a done marker (catch-up), overwrites only that day's partitions, then writes the marker. Silver reuses `silver_transforms.to_silver` unchanged. `seismic` silver is one row per quake (latest `lastupdate` wins), recomputed from all bronze.
- **Idempotency**: deterministic batch keys, per-day dynamic partition overwrite, dedup by `event_id`; reruns give identical output.
- **Cost / Free Tier**: Glue is available on the free account plan but billed from credits (≈0.44 USD per DPU-hour, 1-minute minimum). Capped by `number_of_workers = 2`, `timeout = 60`, one concurrent run; ~5-minute nightly run ≈ 0.07 USD. Requires Glue 5.0 (Python 3.11). The nightly trigger is disabled until backfill is verified (`glue_nightly_enabled`).

## 2. Files Created / Modified
- `src/processing/archiver.py` – raw JSONL batches instead of per-record bronze objects
- `src/processing/batch_rows.py`, `batch_schemas.py`, `src/batch/batch_job.py` – Spark job logic, explicit schemas, entry point
- `scripts/build_glue.sh` – packages and uploads job + dependencies to `s3://<lake>/glue/`
- `infra/glue_batch.tf`, `infra/datalake_layers.tf` – Glue job/role/trigger, raw lifecycle, archiver IAM
- `tests/test_lake.py`, `tests/test_batch.py`, `infra/tests/glue_batch.tftest.hcl`

## 3. Test Documentation Table (Ready for Milestone Report)

| Test ID | Objective | Steps | Expected Result | Actual Result (Log / Screenshot Evidence) |
|---|---|---|---|---|
| **T-BATCH-01** | Archiver groups records per source into JSONL batches; same batch gives same key; bad message blocks only its group | `PYTHONPATH=src:src/ingestion pytest -q tests/test_lake.py` | 28 tests pass; 1 file per source, redelivery overwrites same key | 74 passed (whole suite), 0.33 s |
| **T-BATCH-02** | `parse`/`to_doc` equal `to_silver` plus `ingest_date`; inactive NOAA satellite dropped; seismic one id per quake | `pytest -q tests/test_batch.py` | all pass | included in 74 passed |
| **T-BATCH-03** | Explicit schemas cover every field produced by `to_silver` on all sample datasets; new field is detected | `pytest -q tests/test_batch.py -k schemat` | `unknown_fields == set()` for 6 datasets | included in 74 passed |
| **T-BATCH-04** | Catch-up selects only finished days without done marker | `pytest -q tests/test_batch.py -k pending` | today and marked days skipped | included in 74 passed |
| **T-BATCH-05** | Terraform: cost limits, disabled trigger, raw expiry, archiver env | `terraform -chdir=infra test` | all runs pass | 7 passed, 0 failed |
| **T-BATCH-06** | Live: nightly job twice for one day (idempotency) | Start job with `--day D` twice; compare row counts per dataset | identical counts, marker present | *(paste CloudWatch log `/aws-glue/jobs/output`)* |
| **T-BATCH-07** | Live: legacy backfill completeness | Run `--mode check` after backfill | `CHECK brakujące stare rekordy w bronze: 0` | *(paste log)* |
| **T-BATCH-08** | Live: archiver writes `raw/` batches, no new `bronze/` objects | List `raw/` and `bronze/` after deploy | `.jsonl` files appear, `bronze/` stops growing | *(paste S3 listing)* |
