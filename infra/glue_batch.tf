# Batch: nocny job AWS Glue (Spark) zamienia raw/ (JSONL) w lake/bronze i lake/silver (Parquet).
#
#   bronze_archiver --> s3://<datalake>/raw/ingest_date=D/source=X/<hash>.jsonl   (wygasa po 3 dniach)
#   Glue job batch  --> s3://<datalake>/lake/bronze/  (partycje ingest_date, source)
#                   --> s3://<datalake>/lake/silver/<zbiór>/
#
# Glue jest na liście usług dostępnych na koncie w planie free (Amazon EMR nie jest).
# Płatność: DPU-godziny z kredytów konta; limit kosztów to number_of_workers i timeout poniżej.
# Skrypt i zależności wysyła scripts/build_glue.sh do s3://<datalake>/glue/.
#
# Glue 5.0 (Python 3.11) jest wymagane: schema.to_utc_iso używa datetime.fromisoformat,
# które w Pythonie < 3.11 nie czyta czasów z ułamkiem sekundy (np. seismic "...52.0Z").

locals {
  glue_name = "${var.project}-batch"
  lake_arn  = aws_s3_bucket.datalake.arn
  lake_name = aws_s3_bucket.datalake.bucket
}

# Surowe paczki znikają po 3 dniach (margines na ponowienie nieudanego jobu).
resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "raw-expire"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    expiration {
      days = 3
    }
  }
}

# ------------------------------------------------------------------ rola jobu
data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_job" {
  name               = "${local.glue_name}-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}

# logi CloudWatch i dostęp do katalogu Glue
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_job.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_job" {
  statement {
    sid     = "ReadInputs"
    actions = ["s3:GetObject"]
    resources = [
      "${local.lake_arn}/raw/*",
      "${local.lake_arn}/bronze/*", # stare pliki JSON (backfill)
      "${local.lake_arn}/glue/*",
      "${local.lake_arn}/lake/*",
    ]
  }
  statement {
    sid       = "WriteLake"
    actions   = ["s3:PutObject", "s3:DeleteObject"] # DeleteObject: nadpisanie partycji i pliki _temporary
    resources = ["${local.lake_arn}/lake/*"]
  }
  statement {
    sid       = "List"
    actions   = ["s3:ListBucket"]
    resources = [local.lake_arn]
  }
}

resource "aws_iam_role_policy" "glue_job" {
  name   = "glue-job"
  role   = aws_iam_role.glue_job.id
  policy = data.aws_iam_policy_document.glue_job.json
}

# ------------------------------------------------------------------ job i harmonogram
resource "aws_glue_job" "batch" {
  name              = local.glue_name
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"  # Spark 3.5, Python 3.11 (patrz komentarz na górze)
  worker_type       = "G.1X" # 1 DPU = 4 vCPU, 16 GB
  number_of_workers = 2      # minimum dla Spark; to limit kosztów nocnego jobu
  timeout           = 60     # minuty; twardy bezpiecznik kosztów
  max_retries       = 0      # ponowienie robi kolejny nocny przebieg (nadrabia zaległe dni)

  execution_property {
    max_concurrent_runs = 1 # dwa joby naraz nie nadpiszą sobie partycji
  }

  command {
    name            = "glueetl"
    script_location = "s3://${local.lake_name}/glue/batch_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${local.lake_name}/glue/deps.zip"
    "--bucket"                           = local.lake_name
    "--mode"                             = "nightly"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  depends_on = [aws_iam_role_policy.glue_job, aws_iam_role_policy_attachment.glue_service]
}

variable "glue_nightly_enabled" {
  description = "Czy nocny harmonogram jobu Glue jest włączony. Włącz dopiero po backfillu i kontroli (docs/batch-parquet.md)."
  type        = bool
  default     = false
}

resource "aws_glue_trigger" "nightly" {
  name     = "${local.glue_name}-nightly"
  type     = "SCHEDULED"
  schedule = "cron(0 2 * * ? *)" # 02:00 UTC
  enabled  = var.glue_nightly_enabled

  actions {
    job_name = aws_glue_job.batch.name
  }
}

output "glue_job_name" {
  value = aws_glue_job.batch.name
}
