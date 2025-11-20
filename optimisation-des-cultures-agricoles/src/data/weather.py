import requests
import json
from pathlib import Path

# Casablanca coordinates
lat, lon = 33.5731, -7.5898

# Output folder (inside your project)
OUT_DIR = Path("data/raw/weather")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_weather(start, end, year):
    print(f"\n📥 Fetching weather for Casablanca {year}...")

    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start}&end_date={end}"
        "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
    )

    r = requests.get(url).json()

    weather = []
    hourly = r["hourly"]

    for i in range(len(hourly["time"])):
        weather.append({
            "city": "Casablanca",
            "timestamp": hourly["time"][i],
            "temperature": hourly["temperature_2m"][i],
            "humidity": hourly["relative_humidity_2m"][i],
            "wind_speed": hourly["wind_speed_10m"][i],
            "precipitation": hourly["precipitation"][i]
        })

    out_file = OUT_DIR / f"weather_casablanca_{year}.json"
    
    with open(out_file, "w") as f:
        json.dump(weather, f, indent=2)

    print(f"✔ Exported → {out_file} (READY FOR HDFS)")


# ----- FETCH DATA -----
fetch_weather("2023-01-01", "2023-12-31", 2023)
fetch_weather("2024-01-01", "2024-12-31", 2024)
