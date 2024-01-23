from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, rank, count, year, avg
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from math import cos, sqrt, sin, atan2, radians
import time

def haversine_distance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Differences in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Calculate the distance
    distance = R * c

    return distance

haversine_distance_udf = udf(haversine_distance,FloatType())

# Specify the HDFS path to your CSV file
data = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"
pds = "hdfs://okeanos-master:54310/advancedDB/LAPDs.csv"

# Initialize a Spark session
spark:SparkSession = SparkSession.builder\
            .appName("Query4") \
            .getOrCreate()

start_time = time.time()

df = spark.read.csv(data, header=True, inferSchema=False)
df = df.select(year(to_date(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a")).alias("year"),
                           col("AREA ").cast("integer").alias("AREA"),
                           col("LAT").cast("double").alias("LAT1"),
                           col("LON").cast("double").alias("LON1"),
                           col("Weapon Used Cd").cast("integer").alias("WU"),
                           col("DR_NO").cast("string").alias("CRIME_ID"))\
                            .filter(col("WU")>=100).filter(col("WU")<200)

police_stations = spark.read.csv(pds, header=True, inferSchema=False)
police_stations = police_stations.select(col("X").cast("double").alias("LON2"),
                                         col("Y").cast("double").alias("LAT2"),
                                         col("PREC").cast("integer").alias("AREA"),
                                         col("DIVISION"))

### RESPONSIBLE DIVISION
extended_df = df.join(police_stations, on=["AREA"], how="inner")

extended_df = extended_df.withColumn("distance", haversine_distance_udf(col("LAT1"),col("LON1"),col("LAT2"),col("LON2")))

extended_df.groupBy("year").agg(avg("distance").alias("dist avg"),count("distance").alias("crime_total")).show()

extended_df.groupBy("DIVISION").agg(avg("distance").alias("dist avg"),count("distance").alias("crime_total")).show()
# Calculate and print elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")

### NEAREST DIVISION
extended_df = df.drop("AREA").join(police_stations, how="full")\
    .withColumn("distance", haversine_distance_udf(col("LAT1"),col("LON1"),col("LAT2"),col("LON2")))

windowSpec = Window.partitionBy("CRIME_ID").orderBy(col("distance"))
extended_df = extended_df.withColumn("dist_order", rank().over(windowSpec))\
    .filter(col("dist_order")==1)\
    .select("year","distance","division")
    
extended_df.groupBy("year").agg(avg("distance").alias("dist avg"),count("distance").alias("crime_total")).show()
extended_df.groupBy("DIVISION").agg(avg("distance").alias("dist avg"),count("distance").alias("crime_total")).show()

# Calculate and print elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")


spark.stop()