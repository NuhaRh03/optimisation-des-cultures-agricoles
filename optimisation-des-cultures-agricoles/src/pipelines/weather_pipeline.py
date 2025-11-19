# weather_ingest_all.py

import requests
import json
import subprocess
from datetime import datetime
from config_locations import LOCATIONS

API_URL = "https://archive-api.open-meteo.com/v1/archive"

START = "2024-01-01"
END   = "2024-12-31"

def ingest_city_weather(city, lat, lon):
    print(f"\n====================")
    print(f"🌦 CITY: {city}")
    print(f"====================")

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START,
        "end_date": END,
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain"],
        "timezone": "UTC"
    }

    print("📥 Fetching weather data...")
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    filename = f"{city}_weather_{START}_{END}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f)

    print(f"💾 Saved locally as {filename}")

    # ensure HDFS dir
    hdfs_dir = f"/data/raw/weather/{city}"
    subprocess.run([
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-mkdir", "-p", hdfs_dir
    ])

    print("📂 Copying file into namenode container...")
    subprocess.run(["docker", "cp", filename, f"namenode:/{filename}"], check=True)

    hdfs_path = f"{hdfs_dir}/{filename}"
    print(f"📤 Uploading into HDFS → {hdfs_path}")
    subprocess.run([
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-put", "-f", f"/{filename}", hdfs_path
    ], check=True)

    print(f"🎉 Weather for {city} uploaded to HDFS at: {hdfs_path}")


if __name__ == "__main__":
    print("🚀 Starting WEATHER ingestion for all locations...")
    for city, (lat, lon) in LOCATIONS.items():
        ingest_city_weather(city, lat, lon)
    print("\n✅ All weather data ingested!")
