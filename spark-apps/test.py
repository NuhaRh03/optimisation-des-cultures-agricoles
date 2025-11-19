from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("TestSparkCluster") \
    .getOrCreate()

print("🔥 Spark cluster is working!") 

df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
df.show()

spark.stop()
