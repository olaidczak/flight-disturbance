# Common Event Schema (`Record` in Bronze & Document in Silver)

Code sources of truth: **[`src/common/schema.py`](../src/common/schema.py)** (`Record` envelope for SQS FIFO and S3 Bronze) and **[`src/processing/silver_transforms.py`](../src/processing/silver_transforms.py)** (Bronze -> Silver transformation).

---

## 1. Record Envelope in SQS FIFO and S3 Bronze (`Record`)

Every ingestion poller converts source API payloads into `Record` instances and publishes `Record.to_json()` to the SQS FIFO queue (`flight-disruption-events.fifo`). The `bronze_archiver` Lambda validates the envelope and writes it unchanged to S3.

| Field | Type | Description |
|---|---|---|
| `event_id` | `string` | Deterministic, unique identifier prefixed with `<source>:` (see table below) |
| `source` | `string` | One of: `"opensky"`, `"tomorrow_io"`, `"seismic"`, `"noaa_swpc"` |
| `event_time` | `string` (ISO 8601 UTC) | Timestamp when the event occurred at the source (`YYYY-MM-DDTHH:MM:SSZ`) |
| `ingestion_time` | `string` (ISO 8601 UTC) | Timestamp when the event was fetched by our system (`YYYY-MM-DDTHH:MM:SSZ`) |
| `location` | `object` or `null` | `{"lat": float, "lon": float}` (optionally `"depth"`) or `null` (for global NOAA data) |
| `payload` | `object` | Unmodified raw source data (details below) |

### `event_id` Format, SQS FIFO Attributes, and Deterministic S3 Keys

| Source (`source`) | `event_id` Pattern | `location` |
|---|---|---|
| `opensky` | `opensky:{icao24}:{last_contact_epoch}` | `{"lat": .., "lon": ..}` or `null` |
| `tomorrow_io` | `tomorrow_io:{airport_icao}:{event_time_iso}` | `{"lat": .., "lon": ..}` |
| `seismic` | `seismic:{unid}:{lastupdate}` | `{"lat": .., "lon": .., "depth": ..}` |
| `noaa_swpc` | `noaa_swpc:{wind\|mag}:{satellite}:{event_time_iso}` or `noaa_swpc:kp:{event_time_iso}:{Kp}` | `null` |

- **SQS FIFO Attributes**:
  - `MessageGroupId` = `Record.message_group_id` (`source`),
  - `MessageDeduplicationId` = `Record.deduplication_id` (SHA-256 hash of `event_id`).
- **S3 Bronze Key** (`Record.s3_key_bronze`):
  `bronze/source=<source>/date=<YYYY-MM-DD>/<safe_event_id>.json`
- **S3 Silver Key** (`Record.s3_key_silver`):
  `silver/source=<source>/date=<YYYY-MM-DD>/<safe_event_id>.json`

---

## 2. Raw `payload` (Bronze) vs Cleaned `data` (Silver)

In **Bronze**, `payload` holds the raw source structure. In **Silver**, the output JSON document has the structure:
`{ "event_id", "source", "event_time", "ingestion_time", "location", "data": { ... }, "silver_version": 1 }`.

| Source | Raw `payload` in Bronze (`src/ingestion/`) | Cleaned `data` in Silver (`src/processing/silver_transforms.py`) |
|---|---|---|
| **`opensky`** | `{"state": [<18-element API state vector>], "snapshot_time": <epoch>}` | Named fields mapped via `OPENSKY_FIELDS` (`icao24`, trimmed `callsign` or `null`, `origin_country`, ISO UTC `time_position`, `last_contact`, and `snapshot_time`, `baro_altitude`, `on_ground`, `velocity`, `true_track`, `vertical_rate`, `geo_altitude`, `squawk`, `spi`, `position_source`, `category`; `sensors` dropped). |
| **`tomorrow_io`** | `{"station": "EPWA", "response": <full API JSON>}` | Flattened dictionary `{"station": "EPWA", ...}` with all `response.data.values` keys converted from `camelCase` to `snake_case` (e.g., `wind_speed`, `wind_gust`, `temperature`, `visibility`, `cloud_cover`, `pressure_sea_level`). |
| **`noaa_swpc`** | `{"series": "wind"\|"mag"\|"kp", ...<raw NOAA row>}` | For `wind`/`mag`: rows with `active == False` are filtered out (`None`), `source` renamed to `satellite`, numeric strings cast to `float`. For `kp`: `{"series": "kp", "kp": float, "a_running": float, "station_count": float}`. |
| **`seismic`** | Raw seismic event properties from EMSC | Passed through to `data` until dedicated seismic transformation rules are added. |
