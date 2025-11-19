import os
import math
import requests
import subprocess
from config_locations import LOCATIONS   # SAME CONFIG AS WEATHER
from datetime import datetime

# ---------------------
# TILE SETTINGS
# ---------------------
ZOOM = 12  # Lower = many images, higher = fewer images
TILE_SIZE = 512  # px

# Sentinel tile API (free public mirror)
TILE_URL = "https://services.sentinel-hub.com/ogc/wms/{instance_id}"
INSTANCE = "03b3e2e0-2663-4a21-aa76-f70e9458d15a"  # Public demo instance

# ---------------------
# FUNCTIONS
# ---------------------

def deg2num(lat_deg, lon_deg, zoom):
    """Convert lat/lon to tile numbers"""
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int(
        (1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi)
        / 2.0 * n
    )
    return xtile, ytile


def download_satellite_image(city, lat, lon):
    x, y = deg2num(lat, lon, ZOOM)

    url = f"https://tiles.maps.eox.at/wms?SERVICE=WMS&REQUEST=GetMap" \
          f"&VERSION=1.3.0&LAYERS=sentinel-2-l2a" \
          f"&FORMAT=image/jpeg&WIDTH={TILE_SIZE}&HEIGHT={TILE_SIZE}" \
          f"&CRS=EPSG:3857" \
          f"&BBOX={x},{y},{x+1},{y+1}"

    filename = f"{city}_satellite_{lat}_{lon}.jpg"

    print(f"📥 Downloading satellite image for {city}...")
    resp = requests.get(url, timeout=20)

    if resp.status_code != 200:
        print(f"❌ Failed for {city}. Status: {resp.status_code}")
        return None

    with open(filename, "wb") as f:
        f.write(resp.content)

    print(f"💾 Saved: {filename}")
    return filename


def upload_to_hdfs(filename, city):
    hdfs_path = f"/data/raw/satellite/{city}/{filename}"

    print(f"📂 Copying to namenode...")
    subprocess.run(["docker", "cp", filename, f"namenode:/{filename}"], check=True)

    print(f"📤 Uploading to HDFS...")
    subprocess.run([
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-mkdir", "-p", f"/data/raw/satellite/{city}"
    ])
    subprocess.run([
        "docker", "exec", "namenode",
        "hdfs", "dfs", "-put", "-f", f"/{filename}", hdfs_path
    ], check=True)

    print(f"🎉 Uploaded to {hdfs_path}")


# ---------------------
# MAIN EXECUTION
# ---------------------
print("🚀 Starting SATELLITE ingestion for all locations...\n")

for city, (lat, lon) in LOCATIONS.items():
    print(f"🛰  Processing city: {city.upper()}...")

    filename = download_satellite_image(city, lat, lon)

    if filename:
        upload_to_hdfs(filename, city)

print("\n✅ ALL SATELLITE IMAGES DOWNLOADED & STORED IN HDFS!")
