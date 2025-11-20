from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Casablanca ETL NDVI + Weather") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# ============================
# 1) READ RAW JSON FROM HDFS
# ============================

ndvi_2023 = spark.read.json("hdfs://namenode:9000/data/casablanca/ndvi/ndvi_casablanca_2023.json")
ndvi_2024 = spark.read.json("hdfs://namenode:9000/data/casablanca/ndvi/ndvi_casablanca_2024.json")

weather_2023 = spark.read.json("hdfs://namenode:9000/data/casablanca/weather/weather_casablanca_2023.json")
weather_2024 = spark.read.json("hdfs://namenode:9000/data/casablanca/weather/weather_casablanca_2024.json")

# ============================
# 2) UNION YEARS
# ============================

ndvi = ndvi_2023.union(ndvi_2024)
weather = weather_2023.union(weather_2024)

# ============================
# 3) CLEAN / RENAME / FORMAT
# ============================

# NDVI
ndvi_clean = ndvi.select(
    col("city").alias("city"),
    to_date(col("properties.date")).alias("date"),
    col("properties.ndvi_mean").alias("ndvi_mean")
)

# WEATHER
weather_clean = weather.select(
    col("city").alias("city"),
    to_date(col("timestamp")).alias("date"),
    col("temperature"),
    col("humidity"),
    col("wind_speed"),
    col("precipitation")
)

# ============================
# 4) JOIN NDVI + WEATHER ON DATE
# ============================

final_df = ndvi_clean.join(
    weather_clean,
    on=["city", "date"],
    how="inner"
)

# ============================
# 5) WRITE PARQUET TO HDFS
# ============================

final_df.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/data/casablanca/final_features"
)

print("✔ PySpark ETL Finished Successfully!")
print("✔ Final dataset saved → /data/casablanca/final_features")

spark.stop()
