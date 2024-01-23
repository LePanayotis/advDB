from pyspark.sql import SparkSession
import time

# Specify the HDFS path to your CSV file
data = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"


# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Query1_SQL") \
    .getOrCreate()
start_time = time.time()

# Read the CSV file into a DataFrame
df = spark.read.csv(data, header=True, inferSchema=True)

df.createOrReplaceTempView("crime_data")


res = spark.sql(
    """
SELECT
    year,
    month,
    crime_total,
    ROW_NUMBER() OVER (PARTITION BY year ORDER BY year, crime_total DESC) AS order
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) AS order_within_year
    FROM (
        SELECT
            CAST(SUBSTRING(`Date Rptd`,7,4) AS INT) AS year,
            CAST(SUBSTRING(`Date Rptd`,1,2) AS INT) AS month,
            COUNT(*) AS crime_total
        FROM crime_data
        GROUP BY year, month
        ORDER BY year, crime_total DESC
    ) ranked
) ranked_final
WHERE order_within_year <= 3
    """
).show(n=df.count(), truncate=False)

# Calculate and print elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")

spark.stop()
