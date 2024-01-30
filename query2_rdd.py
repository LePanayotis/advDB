from pyspark import SparkContext, SparkConf
from io import StringIO
import csv
import time

def parse_csv_line(line):
    csv_file = StringIO(line)
    csv_reader = csv.reader(csv_file)
    return next(csv_reader)

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

conf = SparkConf().setAppName("Query2_RDD")
sc = SparkContext(conf=conf)
start_time = time.time()

data = "hdfs://okeanos-master:54310/advancedDB/la-crime.2010-2023.csv"

rdd = sc.textFile(data).map(parse_csv_line)

rdd = rdd.filter(lambda row: row[14] == "101")\
    .map(lambda row: (row[3], row[14], row[15]))


rdd = rdd.map(lambda x: (categorize_time_of_day(x[0]), 1))

rdd = rdd.reduceByKey(lambda x, y: x + y)

rdd = rdd.sortBy(lambda x: x[1], ascending=False)

rdd = rdd.zipWithIndex()\
    .map(lambda x: (x[0][0], x[0][1], x[1] + 1))

print("Query 2 RDD Result:")
for row in rdd.collect():
    print(row)


end_time = time.time()
elapsed_time = end_time - start_time
print(f"Elapsed Time: {elapsed_time} seconds")

sc.stop()
