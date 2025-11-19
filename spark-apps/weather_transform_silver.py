from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("WeatherTransformSilver") \
    .getOrCreate()

# Read raw JSON (bronze)
df = spark.read.json("hdfs://namenode:9000/data/raw/weather/*.json")

# Basic cleaning
df_clean = df.select(
    col("latitude"),
    col("longitude"),
    col("temperature_2m_max").alias("temp_max"),
    col("temperature_2m_min").alias("temp_min"),
    to_date(col("date")).alias("date")
)

# Write cleaned parquet (silver)
df_clean.write.mode("overwrite").parquet("hdfs://namenode:9000/data/processed/weather")

print("✔ Silver layer created!")

spark.stop()
