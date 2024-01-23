from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, count
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import time

def categorize_time_of_day(time):
    hour = int(time[:2])
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    elif 21 <= hour or hour < 5:
        return "Night"
    else:
        return None

categorize_time_of_day_udf = udf(categorize_time_of_day, StringType())

data = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"

spark = SparkSession.builder \
    .appName("Query2") \
    .getOrCreate()
start_time = time.time()

df = spark.read.csv(data, header=True, inferSchema=False)

df = df.filter(col("Premis Cd")=="101").select("TIME OCC")

df = df.withColumn("time", categorize_time_of_day_udf(col("TIME OCC")))\
    .groupBy("time")\
    .agg(count("*").alias("crime_total"))

windowSpec = Window.orderBy(col("crime_total").desc())
df = df.withColumn("order", rank().over(windowSpec))

df.show(n=df.count(), truncate=False)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")

spark.stop()