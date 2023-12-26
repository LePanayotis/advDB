from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, rank, count, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from datetime import datetime
from pyspark.sql.types import StringType
# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Query2") \
    .getOrCreate()

# Specify the HDFS path to your CSV file
data_2010_2019 = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2019.csv"
data_2020_present = "hdfs://okeanos-master:54310/advancedDB/la-crime.2020-present.csv"
df1 = spark.read.csv(data_2010_2019, header=True, inferSchema=True)
df2 = spark.read.csv(data_2020_present, header=True, inferSchema=True)

df = df1.union(df2).filter(col("Premis Cd")==101).select(to_timestamp(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a").alias("DATE OCC"))

# Read the CSV file into a DataFrame
def categorize_time_of_day(timestamp):
    hour = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').hour
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    elif 21 <= hour | hour < 4:
        return "Night"
    else:
        return None

categorize_time_of_day_udf = udf(categorize_time_of_day, StringType())

df = df.withColumn("time", categorize_time_of_day_udf(col("DATE OCC"))).select("time").filter(col("time")!=None)

df = df.groupBy("time").agg(count("*").alias("crime_total"))

windowSpec = Window.orderBy(col("crime_total").desc())
df = df.withColumn("order", rank().over(windowSpec))

df.show(n=df.count(), truncate=False)

output_path = "hdfs://okeanos-master:54310/advancedDB/query2.csv"
df.write.csv(output_path, header=True, mode="overwrite")
# Stop the Spark session
spark.stop()