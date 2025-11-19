import ee
import ee.mapclient

# 1. تهيئة Google Earth Engine (يجب أن تكون قد قمت بالتصديق مسبقًا)
try:
    ee.Initialize()
    print("Google Earth Engine Initialized.")
except Exception as e:
    print(f"Error initializing GEE: {e}")
    print("Please ensure you have authenticated GEE locally.")
    
# ---------------------------------------------------------------------
# 2. تحديد المعلمات
# يجب استبدالها بإحداثيات المنطقة المستهدفة في المغرب (مثال: حوض اللوكوس)
# يمكنك استخدام ملف Shapefile لتحديد قطعة أرض محددة (يتم تحميلها كـ ee.FeatureCollection)
# سنستخدم نقطة اختبار كمثال
AOI_CENTER = [-8.008, 31.629]  # مثال: بالقرب من مراكش
START_DATE = '2025-01-01'
END_DATE = '2025-11-14' # التاريخ الحالي
PARCEL_ID = 'MA_P001'

# ---------------------------------------------------------------------
# 3. دالة لحساب NDVI وتنظيف السحب
def add_ndvi_and_cloud_mask(image):
    # حساب NDVI: (NIR - Red) / (NIR + Red)
    # Sentinel-2 bands: NIR (B8), Red (B4)
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    
    # قناع السحب (Masking): استخدام طبقة SCL (Scene Classification Layer)
    scl = image.select('SCL')
    # القناع الذي يزيل السحب، الظلال، الماء، والـ No Data (SCL values: 0, 1, 3, 8, 9, 10, 11)
    cloud_mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10))
    
    return image.addBands(ndvi).updateMask(cloud_mask)

# ---------------------------------------------------------------------
# 4. بناء المجموعة الزمنية للصور (Image Collection)
s2_collection = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
    .filterDate(START_DATE, END_DATE) \
    .filterBounds(ee.Geometry.Point(AOI_CENTER)) \
    .map(add_ndvi_and_cloud_mask)

# 5. استخلاص قيمة NDVI (مثال: المتوسط لكل قطعة أرض)
# هذه العملية يجب أن تتكرر على جميع قطع الأراضي في منطقتك
def extract_ndvi_for_export(image):
    # الحصول على قيمة NDVI المتوسطة داخل المنطقة (مثال: دائرة نصف قطرها 1000 متر)
    aoi_geometry = ee.Geometry.Point(AOI_CENTER).buffer(1000)
    
    # حساب المتوسط الإحصائي
    mean_ndvi = image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=aoi_geometry,
        scale=10 # دقة 10 متر
    ).get('NDVI')
    
    # الحصول على التاريخ لتوحيد البيانات
    date = image.get('system:time_start')
    
    # تجهيز البيانات للإخراج (لإرسالها لاحقًا إلى HDFS/HBase)
    return ee.Feature(None, {
        'parcel_id': PARCEL_ID,
        'timestamp': ee.Date(date).format('YYYY-MM-dd'),
        'ndvi_mean': mean_ndvi
    })

# تطبيق الدالة على المجموعة
ndvi_features = s2_collection.map(extract_ndvi_for_export).getInfo()

# 6. تصدير البيانات المهيكلة إلى ملف CSV/JSON (جاهز للاستيعاب في Big Data)
import json
print(f"Total NDVI records for {PARCEL_ID}: {len(ndvi_features)}")

# تخزين النتائج في ملف JSON مؤقت
with open('ndvi_exports.json', 'w') as f:
    json.dump(ndvi_features, f)

print("NDVI data exported to ndvi_exports.json, ready for HDFS/HBase ingestion.")