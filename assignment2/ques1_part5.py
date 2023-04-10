import sys
from operator import add
from pyspark.sql import Row
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession\
    .builder\
    .appName("ques1_part5")\
    .getOrCreate()

df = spark.read.option("header", True).csv("/usr/local/spark/input/Parking_Violations_Issued_-_Fiscal_Year_2023.csv")

# creted an abbrevations list of black color from csv list
black_color_abbr = ['BLK', 'BK.', 'BC', 'BLK.',
                    'BK/', 'Black',  'BLAC', 'BCK',  'B LAC', 'BK']

# filtering out all the datapoints using the black abbrevations list
parking_violation_filtered = df.filter(
    df['Vehicle Color'].isin(black_color_abbr))

# selecting all the datapoints with street codes and dropping the null values
data = parking_violation_filtered.select(
    parking_violation_filtered["Street Code1"], parking_violation_filtered["Street Code2"], parking_violation_filtered["Street Code3"]).na.drop()

# converting the datatype of street code values into int datatype
casted_data = data.withColumn(
    "Street Code1", data["Street Code1"].cast("int")).withColumn(
    "Street Code2", data["Street Code2"].cast("int")).withColumn(
    "Street Code3", data["Street Code3"].cast("int"))

transformed_columns = VectorAssembler(inputCols=[
    "Street Code1",
    "Street Code2",
    "Street Code3"
], outputCol='features')

# transitioned columns format
transformed_data = transformed_columns.transform(casted_data)

cluster = KMeans().setK(4).setSeed(1)

# final centroids
final_model = cluster.fit(transformed_data)

# Retrieving  the centroids of the clusters
centroids = final_model.clusterCenters()

# creating a dataframe row with required columns
dataframe_row = spark.createDataFrame(data=[(34510, 10030, 34050)], schema=[
    'Street Code1', 'Street Code2', 'Street Code3'])

dataframe_row = transformed_columns.transform(dataframe_row)

# convert the given data points to our required transitioned_columns format
query_data = final_model.transform(dataframe_row)

# get the prediction (cluster number) of the given point
cluster_number = query_data.select("prediction").first()["prediction"]

predicted_data = final_model.transform(transformed_data)

prediction_data_counts = predicted_data.groupby("prediction").count()

prediction_data_counts_list = []

for row in prediction_data_counts.select('count', 'prediction').collect():
    prediction_data_counts_list.append((row['count'], row['prediction']))

total_data_points = 0

for count, pred in prediction_data_counts_list:
    total_data_points += count

probability_output = 0

for data_points_count, cluster_num in prediction_data_counts_list:
    if cluster_num == cluster_number:
        final_probability = (data_points_count/total_data_points)

print("Probability of a black vehicle getting ticket for  parking illegally at street codes 34510, 10030, 34050 is: ", final_probability)

spark.stop()


