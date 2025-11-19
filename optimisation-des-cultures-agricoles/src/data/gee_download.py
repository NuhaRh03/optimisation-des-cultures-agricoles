import ee
import os

# ---------------------
# 1) AUTH + INIT
# ---------------------
ee.Initialize(project='bigdataagri')   # <-- ديري هنا project-id ديالك

# ---------------------
# 2) PARAMETERS
# ---------------------
LAT = 33.5731
LON = -7.5898
ROI = ee.Geometry.Point([LON, LAT]).buffer(3000)  # دائرة 3km

START = '2024-01-01'
END   = '2024-12-31'

OUT_DIR = "satellite_images"
os.makedirs(OUT_DIR, exist_ok=True)

# ---------------------
# 3) SENTINEL-2 SR COLLECTION
# ---------------------
collection = (
    ee.ImageCollection("COPERNICUS/S2_SR")  # ✔ دائماً فيها QA60
    .filterBounds(ROI)
    .filterDate(START, END)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))  # < 30% clouds
)

print("Number of images:", collection.size().getInfo())

# ---------------------
# 4) FUNCTION TO EXPORT NDVI IMAGE
# ---------------------
def process_image(img):
    ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')

    date = img.date().format("YYYY-MM-dd").getInfo()
    file_path = f"{OUT_DIR}/ndvi_{date}.tif"

    print("Downloading:", file_path)

    url = ndvi.clip(ROI).getDownloadURL({
        'scale': 10,
        'region': ROI,
        'format': 'GEO_TIFF'
    })

    import requests
    r = requests.get(url)

    with open(file_path, "wb") as f:
        f.write(r.content)

# ---------------------
# 5) LOOP OVER IMAGES (mass download)
# ---------------------
img_list = collection.toList(collection.size())

for i in range(collection.size().getInfo()):
    img = ee.Image(img_list.get(i))
    process_image(img)

print("✔ DONE — All satellite NDVI images downloaded!")
