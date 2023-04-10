from operator import add
from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession\
    .builder\
    .appName("ques1_part4")\
    .getOrCreate()

# read the csv as a dataframe
df = spark.read.option("header", True).csv("/usr/local/spark/input/Parking_Violations_Issued_-_Fiscal_Year_2023.csv")

# running the map reduce based on the color of the vehicle
cnt_carcolor_parking_violation = df.rdd.map(lambda x: [x["Vehicle Color"], 1]).reduceByKey(add)

# result of mapreduce is sorting here
cnt_carcolor_parking_violation_desc = cnt_carcolor_parking_violation.sortBy(lambda line: line[1], ascending=False)

# retrieving the topmost value from the sorted output of mapreduce
top_val = spark.sparkContext.parallelize(cnt_carcolor_parking_violation_desc.take(1), 1)

# save the top most value in the outputs folder
top_val.saveAsTextFile("/usr/local/spark/assignment2/outputs/parking_violation_outputs/ques1_part4")

spark.stop()
