from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder \
    .appName("WeatherAnalyticsGold") \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/data/processed/weather")

# Compute daily averages
df_daily = df.groupBy("date").agg(
    avg("temp_max").alias("avg_temp_max"),
    avg("temp_min").alias("avg_temp_min")
)

df_daily.write.mode("overwrite").parquet("hdfs://namenode:9000/data/analytics/weather")

print("✔ Gold analytics computed!")

spark.stop()
