# Data lake. Prefiksy bronze/ silver/ gold/ to tylko konwencja kluczy, nie trzeba ich tworzyć.
resource "aws_s3_bucket" "datalake" {
  # nazwa musi być globalnie unikalna, stąd id konta
  bucket = "${var.project}-datalake-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
