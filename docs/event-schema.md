# Wspólny schemat zdarzenia (SZKIC - do uzgodnienia w zespole)

## Co to jest

To opis **jednej wiadomości JSON**, którą moduły ingestii wysyłają do kolejki SQS FIFO i którą Lambdy-konsumenci potem odbierają. Dzięki temu poller OpenSky, odbiornik WebSocket i konsument piszą/czytają to samo i nie trzeba się domyślać pól. Nie musisz niczego wymyślać od zera: pola wspólne wynikają z raportu, a pola specyficzne dla źródła bierzemy z prawdziwych odpowiedzi API.

## Pola wspólne (każde zdarzenie)

| Pole | Typ | Opis |
|---|---|---|
| `event_id` | string | stabilny, unikalny identyfikator (np. hash: source + source_record_id + event_time); służy też jako deduplication id w SQS |
| `source` | string | `opensky` / `tomorrow_io` / `seismicportal` / `noaa_swpc` |
| `source_record_id` | string | id rekordu w źródle (ICAO24, id trzęsienia ziemi, ...) |
| `event_time` | string, ISO 8601 UTC | czas zdarzenia wg źródła |
| `ingestion_time` | string, ISO 8601 UTC | czas wczytania przez nasz system |
| `update_time` | string, ISO 8601 UTC, opcjonalne | czas aktualizacji (poprawione zdarzenia sejsmiczne) |
| `location` | object `{lat, lon}` lub null | współrzędne, jeśli dotyczy |
| `payload` | object | pola specyficzne dla źródła (poniżej) |

Atrybuty SQS FIFO (poza JSON-em): `MessageGroupId` (np. wartość `source` lub partycja logiczna) i `MessageDeduplicationId` (= `event_id`).

Klucz obiektu w S3 ma być deterministyczny, np. `bronze/<source>/<yyyy>/<mm>/<dd>/<event_id>.json`, żeby ponowny zapis nadpisywał ten sam obiekt (idempotencja).

## Przykład (OpenSky, wartości przykładowe)

```json
{
  "event_id": "opensky-3c6444-2026-11-14T17:05:22Z",
  "source": "opensky",
  "source_record_id": "3c6444",
  "event_time": "2026-11-14T17:05:22Z",
  "ingestion_time": "2026-11-14T17:05:27Z",
  "location": {"lat": 52.17, "lon": 20.96},
  "payload": {
    "altitude": 10500.0,
    "velocity": 230.5,
    "heading": 271.0,
    "vertical_rate": 0.0
  }
}
```

## Payload per źródło (lista pól z raportu, do doprecyzowania)

- **opensky**: `icao24`, `longitude`, `latitude`, `altitude`, `velocity`, `heading`, `vertical_rate`
- **tomorrow_io**: `wind_speed`, `wind_gust`, `visibility`, `precipitation`, `cloud_cover`, `temperature`, `pressure`
- **seismicportal**: `magnitude`, `depth`, `time` (+ id zdarzenia)
- **noaa_swpc**: `solar_wind_speed`, `density`, `bz`, `kp_index`

## Jak to uzupełnić (konkretnie)

1. Każdy pobiera **jedną prawdziwą odpowiedź** ze swojego API i zapisuje ją w `docs/samples/` (np. `opensky.json`, `tomorrow_io.json`, ...).
2. Dla każdego źródła ustalamy, które pola z odpowiedzi trafiają do `payload`, w jakich jednostkach i które mogą być puste.
3. Opcjonalnie zamieniamy to na model Pydantic (walidacja w kodzie), ale to krok późniejszy.
