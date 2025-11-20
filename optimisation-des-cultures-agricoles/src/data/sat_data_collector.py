import ee, json
ee.Initialize(project='bigdataagri')

CITY = "Marrakech"
AOI = [-8.008, 31.629]
region = ee.Geometry.Point(AOI).buffer(3000)

col = (
    ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
    .filterDate("2024-01-01", "2024-12-31")
    .filterBounds(region)
)

def extract(img):
    return ee.Feature(None, {
        "city": CITY,
        "date": img.date().format("YYYY-MM-dd"),
        "cloud_cover": img.get("CLOUD_COVERAGE_ASSESSMENT")
    })

fc = col.map(extract).limit(300)
features = fc.getInfo()

with open("satellite_metadata_marrakech.json","w") as f:
    json.dump(features, f)

print("✔ Satellite metadata Marrakech exported")
