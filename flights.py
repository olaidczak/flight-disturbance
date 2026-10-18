import requests
from datetime import datetime, timezone

url = "https://opensky-network.org/api/states/all"

response = requests.get(url, timeout=30)
response.raise_for_status()

data = response.json()

print(
    "Snapshot time:", datetime.fromtimestamp(data["time"], tz=timezone.utc).isoformat()
)

states = data.get("states") or []
print("Aircraft returned:", len(states))

# OpenSky state vectors are positional arrays.
for flight in states[:10]:
    print(
        {
            "icao24": flight[0],
            "callsign": flight[1].strip() if flight[1] else None,
            "origin_country": flight[2],
            "longitude": flight[5],
            "latitude": flight[6],
            "baro_altitude_m": flight[7],
            "on_ground": flight[8],
            "velocity_mps": flight[9],
            "heading_deg": flight[10],
            "vertical_rate_mps": flight[11],
        }
    )
