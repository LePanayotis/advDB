from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, year, avg, round, desc
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

data = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"
pds = "hdfs://okeanos-master:54310/advancedDB/LAPDs.csv"

spark:SparkSession = SparkSession.builder\
            .appName("Query4a") \
            .getOrCreate()

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
print("--------------------------- \n BROADCAST JOIN \n ---------------------------")
broadcast_join = df.join(police_stations.hint("BROADCAST"), on=["AREA"], how="inner")
broadcast_join.explain("formatted")

# print("--------------------------- \n MERGE JOIN \n ---------------------------")
# merge_join = df.join(police_stations.hint("MERGE"), on=["AREA"], how="inner")
# merge_join.explain("formatted")

# print("--------------------------- \n SHUFFLE_HASH JOIN \n ---------------------------")
# shuffle_hash = df.join(police_stations.hint("SHUFFLE_HASH"), on=["AREA"], how="inner")
# shuffle_hash.explain("formatted")

# print("--------------------------- \n SHUFFLE_REPLICATE_NL JOIN \n ---------------------------")
# shuffle_replicate = df.join(police_stations.hint("SHUFFLE_REPLICATE_NL"), on=["AREA"], how="inner")
# shuffle_replicate.explain("formatted")


joined_dfs = [broadcast_join]#, merge_join, shuffle_hash, shuffle_replicate]


def perform_join(extended_df, start_time):
    

    print("Query 4a Result:")
    extended_df.groupBy("year")\
        .agg(avg("distance").alias("dist avg"),count("distance").alias("crime_total"))\
        .select(col("year"),
                round("dist avg",3).alias("dist avg"),
                col("crime_total"))\
        .orderBy(col("year"))\
        .show(n=extended_df.count())

    extended_df.groupBy("DIVISION")\
        .agg(avg("distance").alias("dist avg"),count("distance").alias("crime_total"))\
        .select(col("DIVISION"),
                round("dist avg",3).alias("dist avg"),
                col("crime_total"))\
        .orderBy(col("crime_total").desc())\
        .show(n=extended_df.count())


    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed Time 4a: {elapsed_time} seconds")

df = broadcast_join

start_time = time.time()
extended_df = df.withColumn("distance", haversine_distance_udf(col("LAT1"),col("LON1"),col("LAT2"),col("LON2")))
perform_join(extended_df, start_time)

spark.stop()