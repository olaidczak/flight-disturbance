import requests
import pandas as pd

BASE_URL = "https://services.swpc.noaa.gov/json/rtsw"

def get_noaa_data(endpoint):
    url = f"{BASE_URL}/{endpoint}"

    response = requests.get(url, timeout=20)
    response.raise_for_status()

    return response.json()

# Solar wind plasma data
plasma = get_noaa_data("rtsw_wind_1m.json")

# Solar wind magnetic field data
magnetic = get_noaa_data("rtsw_mag_1m.json")

# Convert JSON into DataFrames
plasma_df = pd.DataFrame(plasma)
magnetic_df = pd.DataFrame(magnetic)

print("SOLAR WIND PLASMA")
print(plasma_df.tail())

print("\nMAGNETIC FIELD")
print(magnetic_df.tail())