from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, to_date

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


df = df1.union(df2).select(to_date(col("Date Rptd"),"MM/dd/yyyy hh:mm:ss a").alias("Date Rptd"),
                           to_date(col("DATE OCC"),"MM/dd/yyyy hh:mm:ss a").alias("DATE OCC"),
                           col("Vict Age").cast("integer"),
                           col("LAT").cast("double"),
                           col("LON").cast("double"))


df.show()
print("Number of Rows:", df.count())

# Display the data types of each column
print("Data Types of Each Column:")
df.printSchema()

# Stop the Spark session
spark.stop()
