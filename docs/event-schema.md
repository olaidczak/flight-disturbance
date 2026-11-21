# Common Event Schema (`Record` in Bronze, Silver Documents & Speed Layer DynamoDB)

Code sources of truth: **[`src/common/schema.py`](../src/common/schema.py)** (`Record` envelope for SNS/SQS FIFO and S3 Bronze), **[`src/processing/silver_transforms.py`](../src/processing/silver_transforms.py)** (Bronze -> Silver transformation), and **[`src/speed/handler.py`](../src/speed/handler.py)** (Speed Layer real-time items in DynamoDB).

---

## 1. Record Envelope in SNS/SQS FIFO and S3 Bronze (`Record`)

Every ingestion module (`src/ingestion/*`, `src/seismic_ws/main.py`) converts source API payloads into `Record` instances and publishes `Record.to_json()` via `common.sqs.publish()` to the **SNS FIFO topic** (`flight-disruption-events.fifo`, with `QUEUE_URL` fallback). SNS fans out raw messages (`raw_message_delivery = true`) to:
- `flight-disruption-archive-events.fifo` -> `bronze_archiver` (writes unchanged records to S3 `bronze/`),
- `flight-disruption-speed-events.fifo` -> `speed_consumer` (computes near-real-time features into DynamoDB).

| Field | Type | Description |
|---|---|---|
| `event_id` | `string` | Deterministic, unique identifier prefixed with `<source>:` (see table below) |
| `source` | `string` | One of: `"opensky"`, `"tomorrow_io"`, `"seismic"`, `"noaa_swpc"` |
| `event_time` | `string` (ISO 8601 UTC) | Timestamp when the event occurred at the source (`YYYY-MM-DDTHH:MM:SSZ`) |
| `ingestion_time` | `string` (ISO 8601 UTC) | Timestamp when the event was fetched by our system (`YYYY-MM-DDTHH:MM:SSZ`) |
| `location` | `object` or `null` | `{"lat": float, "lon": float}` (optionally `"depth"`) or `null` (for global NOAA data) |
| `payload` | `object` | Unmodified raw source data (details below) |

### `event_id` Format, FIFO Attributes, and Deterministic S3 Keys

| Source (`source`) | Bronze `event_id` Pattern | Silver `silver_event_id` | `location` |
|---|---|---|---|
| `opensky` | `opensky:{icao24}:{last_contact_epoch}` | Same as Bronze | `{"lat": .., "lon": ..}` or `null` |
| `tomorrow_io` | `tomorrow_io:{airport_icao}:{event_time_iso}` | Same as Bronze | `{"lat": .., "lon": ..}` |
| `seismic` | `seismic:{unid}:{lastupdate}` | **`seismic:{unid}`** (`LATEST_WINS`) | `{"lat": .., "lon": .., "depth": ..}` |
| `noaa_swpc` | `noaa_swpc:{wind\|mag}:{satellite}:{event_time_iso}` or `noaa_swpc:kp:{event_time_iso}:{Kp}` | Same as Bronze | `null` |

- **SNS / SQS FIFO Attributes**:
  - `MessageGroupId` = `Record.message_group_id` (`source`),
  - `MessageDeduplicationId` = `Record.deduplication_id` (SHA-256 hash of `event_id`).
- **S3 Bronze Key** (`Record.s3_key_bronze`):
  `bronze/source=<source>/date=<YYYY-MM-DD>/<safe_event_id>.json`
- **S3 Silver Key** (`Record.s3_key_silver`):
  `silver/source=<source>/date=<YYYY-MM-DD>/<safe_silver_event_id>.json`
  *(Exception for `seismic`: Bronze preserves every revision, while Silver stores 1 latest object per `unid` using optimistic locking via `ETag` / `If-Match` comparing full-precision `data.lastupdate`).*

---

## 2. Raw `payload` (Bronze) vs Cleaned `data` (Silver)

In **Bronze**, `payload` holds the raw source structure. In **Silver**, the output JSON document has the structure:
`{ "event_id", "source", "event_time", "ingestion_time", "location", "data": { ... }, "silver_version": 1 }`.

| Source | Raw `payload` in Bronze (`src/ingestion/`, `src/seismic_ws/`) | Cleaned `data` in Silver (`src/processing/silver_transforms.py`) |
|---|---|---|
| **`opensky`** | `{"state": [<18-element API state vector>], "snapshot_time": <epoch>}` | Named fields mapped via `OPENSKY_FIELDS` (`icao24`, trimmed `callsign` or `null`, `origin_country`, ISO UTC `time_position`, `last_contact`, and `snapshot_time`, `baro_altitude`, `on_ground`, `velocity`, `true_track`, `vertical_rate`, `geo_altitude`, `squawk`, `spi`, `position_source`, `category`; `sensors` dropped). |
| **`tomorrow_io`** | `{"station": "EPWA", "response": <full API JSON>}` | Flattened dictionary `{"station": "EPWA", ...}` with all `response.data.values` keys converted from `camelCase` to `snake_case` (e.g., `wind_speed`, `wind_gust`, `temperature`, `visibility`, `cloud_cover`, `pressure_sea_level`). |
| **`noaa_swpc`** | `{"series": "wind"\|"mag"\|"kp", ...<raw NOAA row>}` | For `wind`/`mag`: rows with `active == False` are filtered out (`None`), `source` renamed to `satellite`, numeric strings cast to `float`. For `kp`: `{"series": "kp", "kp": float, "a_running": float, "station_count": float}`. |
| **`seismic`** | Full raw EMSC WebSocket JSON (`{"action": ..., "data": {"properties": {...}}}`; legacy `{"event": ...}` wrapper also supported) | `{"action": ..., **properties}` with `time` normalized to ISO UTC and full-precision `lastupdate` preserved for `LATEST_WINS` version comparison. |

---

## 3. Real-Time View in DynamoDB (`flight-disruption-realtime`, Speed Layer)

The `speed_consumer` Lambda (`src/speed/handler.py`) reuses `to_silver()`, computes `light_features()`, and upserts items (`PAY_PER_REQUEST`, 24h TTL via `expires_at`, conditional write `version_time not_exists OR <= incoming`):

| Source | `entity_id` (PK) | `record_type` (SK) | Key Stored Attributes |
|---|---|---|---|
| `opensky` | `flight#<icao24>` | `LATEST` | `features` (velocity, altitudes, vertical_rate, `nearest_weather_station`, `weather_station_distance_km`, `weather_age_minutes`, `weather_stale`, `noaa_stale`, `missing_features`) + `context` (latest weather + NOAA features) |
| `tomorrow_io` | `weather#<station>` | `LATEST` | `features` (temperature, wind_speed, wind_gust, visibility, precipitation, cloud_cover, weather_code) |
| `noaa_swpc` | `noaa#<series>` | `LATEST` | `features` (series, satellite, kp, proton_speed, proton_density, bt, bx/by/bz_gsm) |
| `seismic` | `seismic#<unid>` | `LATEST` | `features` (magnitude, depth, region, event_type), `version_time = data.lastupdate` |
