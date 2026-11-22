#!/usr/bin/env python3
"""Uruchamia poller lokalnie w pętli i wysyła rekordy do tej samej kolejki SQS co Lambda.

Po co: gdy poller nie działa w chmurze (np. OpenSky nie odpowiada z adresów AWS), ten sam kod
może pobierać dane z Waszego komputera. Dalej płynie to tak samo: SQS -> archiver -> bronze -> silver.

Użycie (z korzenia repo, w aktywnym .venv; wymaga boto3 i requests oraz zalogowanego AWS CLI):

  export AWS_REGION=eu-north-1
  export QUEUE_URL=https://sqs.eu-north-1.amazonaws.com/592964949672/flight-disruption-events.fifo
  export OPENSKY_SECRET_ARN=arn:aws:secretsmanager:eu-north-1:592964949672:secret:flight-disruption/opensky-...
  caffeinate -i python scripts/run_poller_local.py opensky --interval 60

`caffeinate -i` (macOS) nie pozwala komputerowi zasnąć. Ctrl+C kończy pętlę.
Jeśli Lambda i skrypt działają jednocześnie, nic się nie psuje: ten sam rekord ma ten sam event_id,
więc duplikat odrzuci SQS albo nadpisze ten sam klucz w S3.
"""
from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

POLLERS = ("opensky", "noaa_swpc", "tomorrow_io")


def load_poller(name: str):
    """Szuka modułu pollera w src/ingestion, src/ingestion/pollers albo płasko."""
    tried = []
    for pkg in ("ingestion", "ingestion.pollers", "pollers", ""):
        mod = f"{pkg}.{name}" if pkg else name
        tried.append(mod)
        try:
            return importlib.import_module(mod)
        except ModuleNotFoundError as e:
            if e.name and (mod == e.name or mod.startswith(e.name + ".")):
                continue
            raise
    raise SystemExit(f"Nie znaleziono pollera '{name}'. Próbowano: {', '.join(tried)}")


def check_env() -> None:
    missing = []
    if not os.environ.get("QUEUE_URL"):
        missing.append("QUEUE_URL")
    if not (os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")):
        missing.append("AWS_REGION")
    if missing:
        raise SystemExit("Brak zmiennych środowiskowych: " + ", ".join(missing) + " (patrz opis na górze pliku)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("poller", choices=POLLERS)
    ap.add_argument("--interval", type=float, default=60.0, help="odstęp między wywołaniami w sekundach (domyślnie 60)")
    ap.add_argument("--iterations", type=int, default=0, help="liczba wywołań, 0 = bez końca")
    args = ap.parse_args()

    check_env()
    # Poller czyta sekret z SECRET_NAME (tak ustawia go Terraform dla Lambdy).
    if args.poller == "opensky" and os.environ.get("OPENSKY_SECRET_ARN"):
        os.environ.setdefault("SECRET_NAME", os.environ["OPENSKY_SECRET_ARN"])
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    poller = load_poller(args.poller)

    n = 0
    try:
        while True:
            n += 1
            started = time.monotonic()
            try:
                result = poller.lambda_handler()
                logging.info("%s #%d: %s", args.poller, n, result)
            except Exception:  # noqa: BLE001 - jedno nieudane wywołanie nie kończy pętli
                logging.exception("%s #%d: błąd, następna próba za %.0f s", args.poller, n, args.interval)
            if args.iterations and n >= args.iterations:
                break
            time.sleep(max(0.0, args.interval - (time.monotonic() - started)))
    except KeyboardInterrupt:
        logging.info("Zatrzymano po %d wywołaniach", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
