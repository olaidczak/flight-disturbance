import requests
from datetime import datetime
import os 
from dotenv import load_dotenv
load_dotenv()

# https://docs.tomorrow.io/

key = os.getenv("WEATHER_KEY")
url = "https://api.tomorrow.io/v4/weather/realtime"

params = {
    "location": "Warsaw",
    "units": "metric",
    "apikey": key,
}

headers = {
    "accept": "application/json"
}
response = requests.get(
        url,
        params=params,
        headers=headers,
        timeout=15
    )
result = response.json()
time=result.get("data").get('time')
print(f"time = {time}")
print(result)
