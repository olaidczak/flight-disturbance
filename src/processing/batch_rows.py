"""Funkcje dla jobu Glue/Spark (src/batch/batch_job.py). Bez importu pyspark, więc da się je testować offline."""
from __future__ import annotations

import json

from common.schema import Record
from processing.silver_transforms import to_silver

COLS = ["event_id", "source", "event_time", "ingestion_time", "location", "payload"]


def parse(line: str) -> tuple:
    """Jedna linia JSON (Record.to_json()) -> krotka kolumn bronze. location i payload jako tekst JSON."""
    r = json.loads(line)
    return (r["event_id"], r["source"], r["event_time"], r["ingestion_time"],
            json.dumps(r.get("location")), json.dumps(r["payload"], separators=(",", ":")))


def to_doc(row) -> str | None:
    """Wiersz bronze -> dokument silver jako tekst JSON (None = rekord odrzucony przez czyszczenie)."""
    rec = Record.from_json(json.dumps({
        "event_id": row.event_id, "source": row.source, "event_time": row.event_time,
        "ingestion_time": row.ingestion_time, "location": json.loads(row.location),
        "payload": json.loads(row.payload)}))
    doc = to_silver(rec)
    if doc is None:
        return None
    doc["ingest_date"] = row.ingest_date
    return json.dumps(doc, ensure_ascii=False)


def pending_days(raw_days, done_days, today: str) -> list[str]:
    """Dni z raw/ bez znacznika _done, tylko zakończone (przed `today`), od najstarszego."""
    return sorted(d for d in set(raw_days) - set(done_days) if d < today)
