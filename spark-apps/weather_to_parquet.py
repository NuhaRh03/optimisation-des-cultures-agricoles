from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("WeatherToParquet") \
    .getOrCreate()

df = spark.read.json("hdfs://namenode:9000/data/raw/weather/*.json")

df.write.mode("overwrite").parquet("hdfs://namenode:9000/data/processed/weather")

print("Weather data saved as Parquet!")

spark.stop()
