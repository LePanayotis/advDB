from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, 
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import datetime
from pyspark.sql.functions import col
# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Query0") \
    .getOrCreate()

# Specify the HDFS path to your CSV file
data_2010_2019 = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2019.csv"
data_2020_present = "hdfs://okeanos-master:54310/advancedDB/la-crime.2020-present.csv"

# Read the CSV file into a DataFrame


df1 = spark.read.csv(data_2010_2019, header=True, inferSchema=True)
df2 = spark.read.csv(data_2020_present, header=True, inferSchema=True)

df = df1.union(df2).filter(col("Premis Cd")==101)
                   .select(to_timestamp(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a").alias("DATE OCC"))

def 

df = df.groupBy("year", "month").agg(count("*").alias("crime_total"))

windowSpec = Window.partitionBy("year").orderBy("year", col("crime_total").desc())
df = df.withColumn("order_within_year", rank().over(windowSpec))

df = df.filter(col("order_within_year") <= 3)
df = df.drop("order_within_year")
windowSpec = Window.partitionBy("year").orderBy("year", col("crime_total").desc())
df = df.withColumn("order", row_number().over(windowSpec))

df = df.orderBy("year", col("crime_total").desc())

df.show(n=df.count(), truncate=False)

output_path = "hdfs://okeanos-master:54310/advancedDB/query1.csv"
df.write.csv(output_path, header=True, mode="overwrite")
# Stop the Spark session
spark.stop()