from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, rank, count, year
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import time

spark = SparkSession.builder \
    .appName("Query3") \
    .getOrCreate()
start_time = time.time()

def income_to_int(income):
    return int(income.replace("$", "").replace(",", ""))

income_to_int_udf = udf(income_to_int, IntegerType())

main_dataset = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"
wealth_by_zip = "hdfs://okeanos-master:54310/advancedDB/income/LA_income_2015.csv"
geocoding = "hdfs://okeanos-master:54310/advancedDB/geocoding.csv"


gc = spark.read.csv(geocoding, header=True, inferSchema=False)
df = spark.read.csv(main_dataset, header=True, inferSchema=False)
income_df = spark.read.csv(wealth_by_zip, header=True, inferSchema=False)


gc = gc.select(col("LAT").cast("double"),
               col("LON").cast("double"),
               col("ZIPCode").alias("Zip Code"))

df = df.select(year(to_timestamp(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a")).alias("year"),
                           col("Vict Descent").cast("string"),
                           col("LAT").cast("double"),
                           col("LON").cast("double")) \
                           .filter(col("year")=="2015") \
                            .filter(col("Vict Descent")!="NULL")

income_df = income_df.withColumn("Median Income",income_to_int_udf(col("Estimated Median Income")))\
    .select(col("Zip Code").cast("string"),
            col("Median Income").cast("integer"))


windowSpec = Window.orderBy(col("Median Income"))
income_incr = income_df.orderBy(col("Median Income"))\
    .withColumn("order", rank().over(windowSpec))\
    .filter(col("order")<=3)

windowSpec = Window.orderBy(col("Median Income").desc())
income_desc = income_df.orderBy(col("Median Income").desc())\
    .withColumn("order", rank().over(windowSpec))\
    .filter(col("order")<=3)

income_df = income_incr.union(income_desc).drop(col("order"))

gc = gc.join(income_df, on=["Zip Code"], how="inner")
gc.explain("formatted")

print("Query 3 Result:")
df = df.join(gc, on=["LAT","LON"], how="inner")\
    .select("Vict Descent")\
    .groupBy(col("Vict Descent"))\
    .agg(count("*").alias("crime_total"))\
    .orderBy(col("crime_total").desc())\
    .show(n=df.count(), truncate=False)


end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")

spark.stop()