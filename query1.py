from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, count, row_number, rank
from pyspark.sql.window import Window
import time

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Query1") \
    .getOrCreate()
start_time = time.time()
# Specify the HDFS path to your CSV file
data = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(data, header=True, inferSchema=True)

df = df.select(to_date(col("Date Rptd"),"MM/dd/yyyy hh:mm:ss a").alias("Date Rptd"))

windowSpec = Window.partitionBy("year").orderBy("year", col("crime_total").desc())

df = df.withColumn("year", year("Date Rptd"))\
    .withColumn("month", month("Date Rptd"))\
    .groupBy("year", "month")\
    .agg(count("*").alias("crime_total"))\
    .withColumn("order_within_year", rank().over(windowSpec))\
    .filter(col("order_within_year") <= 3)\
    .drop("order_within_year")

windowSpec = Window.partitionBy("year").orderBy("year", col("crime_total").desc())

df = df.withColumn("order", row_number().over(windowSpec))\
    .orderBy("year", col("crime_total").desc())\
    .show(n=df.count(), truncate=False)

# Calculate and print elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")

# Stop the Spark session
spark.stop()