import sys
from operator import add
from pyspark.sql import Row
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from collections import defaultdict
from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession\
    .builder\
    .appName("ques2_part2")\
    .getOrCreate()

shot_logs_df = spark.read.option("header", True).csv("/usr/local/spark/input/shot_logs.csv")

#dropping the null values from the 
shot_logs_data = shot_logs_df.select(shot_logs_df["SHOT_DIST"], shot_logs_df["CLOSE_DEF_DIST"], shot_logs_df["SHOT_CLOCK"], shot_logs_df["player_name"], shot_logs_df["SHOT_RESULT"]).na.drop()

# converting the values into float datatype
shot_logs_data_casted = shot_logs_data.withColumn("SHOT_DIST", shot_logs_data["SHOT_DIST"].cast("float")).withColumn("CLOSE_DEF_DIST", shot_logs_data["CLOSE_DEF_DIST"].cast("float")).withColumn("SHOT_CLOCK", shot_logs_data["SHOT_CLOCK"].cast("float"))

#KMeans requires a specific format so converting it using VectorAssembler
transitioned_columns = VectorAssembler(inputCols=[
    "SHOT_DIST",
    "CLOSE_DEF_DIST",
    "SHOT_CLOCK"
], outputCol='features')

#returning the result into the transitioned columns
transitioned_data = transitioned_columns.transform(shot_logs_data_casted)

#Random seed
cluster = KMeans().setK(4).setSeed(1)

#final centroids
model_with_final_centroids = cluster.fit(transitioned_data)

# transform to get final prediction
final_prediction = model_with_final_centroids.transform(transitioned_data)

# Getting the centroids of all the clusters formed
centroids = model_with_final_centroids.clusterCenters()

james_harden_zone_count = defaultdict(lambda: 0)
chris_paul_zone_count = defaultdict(lambda: 0)
stephen_curry_zone_count = defaultdict(lambda: 0)
lebron_james_zone_count = defaultdict(lambda: 0)

james_harden_total = 0
chris_paul_total = 0
stephen_curry_total = 0
lebron_james_total = 0

# Comfort zone for James Harden
for row_data in final_prediction.collect():
    if row_data["player_name"].lower() == "james harden":
        if row_data["SHOT_RESULT"].lower() == "made":
            james_harden_zone_count[row_data["prediction"]] += 1
        james_harden_total += 1

# Comfort zone for Chris Paul
for row_data in final_prediction.collect():
    if row_data["player_name"].lower() == "chris paul":
        if row_data["SHOT_RESULT"].lower() == "made":
            chris_paul_zone_count[row_data["prediction"]] += 1
        chris_paul_total += 1

# Comfort zone for Stephen Curry
for row_data in final_prediction.collect():
    if row_data["player_name"].lower() == "stephen curry":
        if row_data["SHOT_RESULT"].lower() == "made":
            stephen_curry_zone_count[row_data["prediction"]] += 1
        stephen_curry_total += 1 


# Comfort zone for Lebron James
for row_data in final_prediction.collect():
    if row_data["player_name"].lower() == "lebron james":
        if row_data["SHOT_RESULT"].lower() == "made":
            lebron_james_zone_count[row_data["prediction"]] += 1
        lebron_james_total += 1

#finding the hit rate and storing it to the original dictionaries
for key in james_harden_zone_count.keys():
    james_harden_zone_count[key] = james_harden_zone_count[key]/james_harden_total

for key in chris_paul_zone_count.keys():
    chris_paul_zone_count[key] = chris_paul_zone_count[key]/james_harden_total

for key in stephen_curry_zone_count.keys():
    stephen_curry_zone_count[key] = stephen_curry_zone_count[key]/james_harden_total

for key in lebron_james_zone_count.keys():
    lebron_james_zone_count[key] = lebron_james_zone_count[key]/james_harden_total


# finding the best zone for James Harden
james_harden_comfortable_zone = max(james_harden_zone_count, key=james_harden_zone_count.get)
chris_paul_comfortable_zone = max(chris_paul_zone_count, key=chris_paul_zone_count.get)
stephen_curry_comfortable_zone = max(stephen_curry_zone_count, key=stephen_curry_zone_count.get)
lebron_james_comfortable_zone = max(lebron_james_zone_count, key=lebron_james_zone_count.get)

print("Comfortable zone for James Harden is : ", james_harden_comfortable_zone)
print("Comfortable zone for Chris Paul is : ", chris_paul_comfortable_zone)
print("Comfortable zone for Stephen Curry is : ", stephen_curry_comfortable_zone)
print("Comfortable zone for Lebron James is : ", lebron_james_comfortable_zone)

spark.stop()
