#!/usr/bin/env bash
# Ustawia zmienne i uruchamia run_poller_local.py (Linux/macOS, bez Terraform).
# Uruchamiaj z głównego katalogu repo: bash scripts/run_poller_local.sh opensky --interval 60
# Każdą zmienną można nadpisać wcześniej, np. AWS_REGION=eu-central-1 bash scripts/run_poller_local.sh ...
set -euo pipefail

export AWS_REGION="${AWS_REGION:-eu-north-1}"
export QUEUE_URL="${QUEUE_URL:-https://sqs.eu-north-1.amazonaws.com/592964949672/flight-disruption-events.fifo}"
export OPENSKY_SECRET_ARN="${OPENSKY_SECRET_ARN:-flight-disruption/opensky}"  # nazwa sekretu też działa

exec python "$(dirname "$0")/run_poller_local.py" "$@"
