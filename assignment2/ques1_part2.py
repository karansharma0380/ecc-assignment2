from operator import add
from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession\
    .builder\
    .appName("ques1_part2")\
    .getOrCreate()

# read the csv as a data frame
df = spark.read.option("header", True).csv("/usr/local/spark/input/Parking_Violations_Issued_-_Fiscal_Year_2023.csv")

# running  mapreduce based upon the issue date
cnt_date_parking_violation = df.rdd.map(lambda x: [(x["Issue Date"].split("/")[2], x["Vehicle Body Type"]), 1]).reduceByKey(add)

# result of mapreduce is sorting here
cnt_date_violation_desc = cnt_date_parking_violation.sortBy(lambda line: line[1], ascending=False)

# retrieving the topmost value from the sorted output of mapreduce
top_val = spark.sparkContext.parallelize(cnt_date_violation_desc.take(1), 1)

# save the top most value in the outputs folder
top_val.saveAsTextFile("/usr/local/spark/assignment2/outputs/parking_violation_outputs/ques1_part2")

spark.stop()
