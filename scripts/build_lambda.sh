#!/usr/bin/env bash
# Pakuje src/ (+ zależności z requirements.txt) do build/lambda.zip.
# Uruchamiaj z głównego katalogu repo: bash scripts/build_lambda.sh
set -euo pipefail

rm -rf build && mkdir -p build/package

# Zależności tylko jeśli są (boto3 jest już w runtime Lambdy, nie dodawaj go)
if [ -s requirements.txt ]; then
  pip install -r requirements.txt -t build/package \
    --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12 --implementation cp
fi

cp -r src/* build/package/
find build/package -name "__pycache__" -type d -prune -exec rm -rf {} +

(cd build/package && zip -qr ../lambda.zip .)
echo "OK: build/lambda.zip ($(du -h build/lambda.zip | cut -f1))"
