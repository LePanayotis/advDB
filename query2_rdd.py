from pyspark import SparkContext, SparkConf
from io import StringIO
import csv
import time

def parse_csv_line(line):
    # Using StringIO to create a file-like object for csv.reader
    csv_file = StringIO(line)
    # Using csv.reader to handle CSV parsing
    csv_reader = csv.reader(csv_file)
    # Returning the parsed row as a list
    return next(csv_reader)

# Define a function to categorize time of day
def categorize_time_of_day(time):
    hour = int(time)
    if 500 <= hour < 1200:
        return "Morning"
    elif 1200 <= hour < 1700:
        return "Afternoon"
    elif 1700 <= hour < 2100:
        return "Evening"
    elif 2100 <= hour or hour < 500:
        return "Night"
    else:
        return None
# Initialize a Spark context
conf = SparkConf().setAppName("Query2_RDD")
sc = SparkContext(conf=conf)
start_time = time.time()

# Specify the HDFS path to your CSV files
data = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"
# Load data into RDDs
#rdd = sc.textFile(data).map(lambda line: line.split(","))
rdd = sc.textFile(data).map(parse_csv_line)

# Union the RDDs and filter based on Premis Cd
rdd = rdd.filter(lambda row: row[14] == "101")\
    .map(lambda row: (row[3], row[14], row[15]))


# Apply the categorize_time_of_day function to the RDD
rdd = rdd.map(lambda x: (categorize_time_of_day(x[0]), 1))
# Reduce by key to get the count of crimes for each time category
rdd = rdd.reduceByKey(lambda x, y: x + y)
# Sort the result by crime_total in descending order
rdd = rdd.sortBy(lambda x: x[1], ascending=False)
# Add rank to the result
rdd = rdd.zipWithIndex()\
    .map(lambda x: (x[0][0], x[0][1], x[1] + 1))

# Display the result
for row in rdd.collect():
    print(row)


# Calculate and print elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")

# Stop the Spark context
sc.stop()
