"""SQS consumer for validation, transformation and light features."""
from __future__ import annotations

import logging
import os
import time
from decimal import Decimal

from common.schema import Record, now_utc_iso
from processing.silver_transforms import to_silver
from speed.features import age_minutes, light_features, nearest_station

logger = logging.getLogger(__name__)

TRANSFORM_VERSION = 1
FEATURE_VERSION = 1
_table = None


def get_table():
    global _table
    if _table is None:
        import boto3

        _table = boto3.resource("dynamodb").Table(os.environ["REALTIME_TABLE"])
    return _table


def _decimal(value):
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_decimal(item) for item in value]
    if isinstance(value, dict):
        return {key: _decimal(item) for key, item in value.items()}
    return value


def _key(record: Record, data: dict) -> str:
    if record.source == "opensky":
        return f"flight#{data['icao24']}"
    if record.source == "tomorrow_io":
        return f"weather#{data['station']}"
    if record.source == "noaa_swpc":
        return f"noaa#{data['series']}"
    if record.source == "seismic":
        return f"seismic#{data['event_id']}"
    raise ValueError(f"Unsupported source: {record.source}")


def _get(entity_id: str) -> dict | None:
    response = get_table().get_item(Key={"entity_id": entity_id, "record_type": "LATEST"})
    return response.get("Item")


def _context(record: Record, features: dict) -> dict:
    if record.source != "opensky":
        return {}

    station, distance = nearest_station(record.location)
    related = {
        "weather": _get(f"weather#{station}") if station else None,
        "noaa_wind": _get("noaa#wind"),
        "noaa_mag": _get("noaa#mag"),
        "noaa_kp": _get("noaa#kp"),
    }
    weather_age = age_minutes(record.event_time, related["weather"].get("event_time") if related["weather"] else None)
    noaa_ages = [
        age_minutes(record.event_time, item.get("event_time"))
        for name, item in related.items()
        if name.startswith("noaa_") and item
    ]
    missing = [name for name, item in related.items() if item is None]
    features.update({
        "nearest_weather_station": station,
        "weather_station_distance_km": distance,
        "weather_age_minutes": weather_age,
        "weather_stale": weather_age is None or weather_age > int(os.environ.get("WEATHER_MAX_AGE_MIN", "30")),
        "noaa_stale": not noaa_ages or max(noaa_ages) > int(os.environ.get("NOAA_MAX_AGE_MIN", "30")),
        "missing_features": missing,
    })
    return {name: item.get("features") if item else None for name, item in related.items()}


def process(body: str | bytes) -> str:
    record = Record.from_json(body)
    transformed = to_silver(record)
    if transformed is None:
        return "skipped"

    data = transformed["data"]
    features = light_features(record.source, data)
    context = _context(record, features)
    item = _decimal({
        "entity_id": _key(record, data),
        "record_type": "LATEST",
        "source": record.source,
        "event_id": record.event_id,
        "event_time": record.event_time,
        "updated_at": now_utc_iso(),
        "expires_at": int(time.time()) + int(os.environ.get("TTL_HOURS", "24")) * 3600,
        "location": record.location,
        "data": data,
        "features": features,
        "context": context,
        "transform_version": TRANSFORM_VERSION,
        "feature_version": FEATURE_VERSION,
    })
    table = get_table()
    try:
        from boto3.dynamodb.conditions import Attr

        table.put_item(
            Item=item,
            ConditionExpression=Attr("event_time").not_exists() | Attr("event_time").lte(record.event_time),
        )
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return "stale"
    return "written"


def lambda_handler(event: dict, context=None) -> dict:
    failures = []
    failed_groups = set()
    counts = {"written": 0, "skipped": 0, "stale": 0}

    for message in event.get("Records", []):
        group = message.get("attributes", {}).get("MessageGroupId", "")
        if group in failed_groups:
            failures.append({"itemIdentifier": message["messageId"]})
            continue
        try:
            counts[process(message["body"])] += 1
        except Exception:
            logger.exception("Speed consumer failed for message %s", message.get("messageId"))
            failures.append({"itemIdentifier": message["messageId"]})
            failed_groups.add(group)

    logger.info("Speed consumer: %s, failures: %d", counts, len(failures))
    return {"batchItemFailures": failures}
