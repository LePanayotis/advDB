from pyspark.sql import SparkSession

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

df = df1.union(df2)

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



output_path = "hdfs://okeanos-master:54310/advancedDB/query1_sql.csv"
res.write.csv(output_path, header=True, mode="overwrite")

spark.stop()
