from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip

# --------------------------
#  INITIALISATION SPARK
# --------------------------
spark = SparkSession.builder \
    .appName("WeatherTransform") \
    .getOrCreate()

# --------------------------
#  PATHS HDFS
# --------------------------
RAW_PATH = "hdfs://namenode:9000/data/raw/weather"
OUTPUT_PATH = "hdfs://namenode:9000/data/curated/weather"

# --------------------------
#  LECTURE JSON BRUT
# --------------------------
df_raw = spark.read.json(RAW_PATH)

# --------------------------
#  TRANSFORMATION
#  On aligne time[], temperature_2m[], rain[], humidity[]
# --------------------------
df_hourly = (
    df_raw
    .selectExpr(
        "latitude",
        "longitude",
        "inline(arrays_zip(hourly.time, hourly.temperature_2m, hourly.rain, hourly.relative_humidity_2m)) as h"
    )
    .select(
        col("latitude"),
        col("longitude"),
        col("h.time").alias("timestamp"),
        col("h.temperature_2m").alias("temperature"),
        col("h.rain").alias("rain_mm"),
        col("h.relative_humidity_2m").alias("humidity")
    )
)

# --------------------------
#  ÉCRITURE PARQUET
# --------------------------
df_hourly.write.mode("overwrite").parquet(OUTPUT_PATH)

print("🎉 Weather data transformed and saved to:", OUTPUT_PATH)

spark.stop()
