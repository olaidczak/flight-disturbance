#!/usr/bin/env bash
# Pakuje job Glue (Spark) i jego zależności i wysyła do s3://<bucket>/glue/.
# Uruchamiaj z głównego katalogu repo: bash scripts/build_glue.sh <bucket>
set -euo pipefail

BUCKET="${1:?Użycie: bash scripts/build_glue.sh <bucket>}"

rm -rf build/glue && mkdir -p build/glue/pkg/processing
cp -r src/common build/glue/pkg/
cp src/processing/__init__.py src/processing/silver_transforms.py src/processing/batch_rows.py \
   src/processing/batch_schemas.py build/glue/pkg/processing/
find build/glue -name "__pycache__" -type d -prune -exec rm -rf {} +
(cd build/glue/pkg && python -m zipfile -c ../deps.zip .)   # zip nie zawsze jest w Git Bash

aws s3 cp build/glue/deps.zip "s3://$BUCKET/glue/deps.zip"
aws s3 cp src/batch/batch_job.py "s3://$BUCKET/glue/batch_job.py"
echo "OK: wysłano deps.zip i batch_job.py do s3://$BUCKET/glue/"
