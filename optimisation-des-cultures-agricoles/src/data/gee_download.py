import ee
import json
from pathlib import Path

# Initialize Earth Engine
ee.Initialize(project='bigdataagri')

# Casablanca coordinates
lat, lon = 33.5731, -7.5898
CITY = "Casablanca"

# Output directory
OUT_DIR = Path("data/raw/satellite")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def extract_ndvi(start, end, year):
    print(f"\n🛰 Extracting NDVI for Casablanca {year}...")

    # AOI = منطقه دائرية (3 KM)
    region = ee.Geometry.Point([lon, lat]).buffer(3000)

    # Build NDVI collection
    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start, end)
        .filterBounds(region)
        .map(lambda img: img.addBands(
            img.normalizedDifference(["B8", "B4"]).rename("NDVI")
        ))
    )

    # Extract NDVI mean per image
    def extract(img):
        mean_ndvi = img.select("NDVI").reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region,
            scale=10,
            maxPixels=1e13
        ).get("NDVI")

        date = img.date().format("YYYY-MM-dd")

        return ee.Feature(None, {
            "city": CITY,
            "date": date,
            "ndvi_mean": mean_ndvi
        })

    # Create FeatureCollection & get JSON
    fc = collection.map(extract).limit(500)  # Enough for 2 years
    features = fc.getInfo()

    # Save JSON
    out_file = OUT_DIR / f"ndvi_casablanca_{year}.json"
    with open(out_file, "w") as f:
        json.dump(features, f, indent=2)

    print(f"✔ Saved → {out_file}")


# ---- RUN EXTRACTIONS ----
extract_ndvi("2023-01-01", "2023-12-31", 2023)
extract_ndvi("2024-01-01", "2024-12-31", 2024)
