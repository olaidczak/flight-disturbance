"""Klucze API: w chmurze z Secrets Managera, lokalnie ze zmiennych środowiskowych (.env)."""
from __future__ import annotations

import json
import os

_cache: dict[str, dict] = {}


def get_secret(name: str | None = None) -> dict | None:
    """Zwraca sekret jako dict albo None, jeśli nie ma go ani w chmurze, ani lokalnie.

    name: nazwa sekretu; domyślnie zmienna SECRET_NAME (ustawiana przez Terraform).
    Pusta nazwa => nie ma czego pobierać z chmury (np. NOAA nie ma klucza).
    """
    name = name if name is not None else os.environ.get("SECRET_NAME", "")
    if not name:
        return None
    if name in _cache:
        return _cache[name]

    import boto3

    client = boto3.client("secretsmanager")
    value = json.loads(client.get_secret_value(SecretId=name)["SecretString"])
    _cache[name] = value  # cache na czas życia rozgrzanej Lambdy
    return value


def tomorrow_api_keys() -> list[str]:
    """Klucze Tomorrow.io: {"api_keys": [...]} w chmurze, TOMORROW_API_KEYS="k1,k2" lokalnie."""
    secret = get_secret()
    if secret:
        return list(secret["api_keys"])
    raw = os.environ.get("TOMORROW_API_KEYS") or os.environ.get("TOMORROW_API_KEY", "")
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    if not keys:
        raise RuntimeError("Brak kluczy Tomorrow.io (SECRET_NAME albo TOMORROW_API_KEYS)")
    return keys


def opensky_credentials() -> tuple[str, str] | None:
    """(client_id, client_secret) albo None => zapytania anonimowe (400 kredytów dziennie)."""
    secret = get_secret()
    if secret:
        return secret["client_id"], secret["client_secret"]
    cid, csec = os.environ.get("OPENSKY_CLIENT_ID"), os.environ.get("OPENSKY_CLIENT_SECRET")
    return (cid, csec) if cid and csec else None
