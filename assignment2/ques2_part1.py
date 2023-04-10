from operator import add
from pyspark.sql import Row
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from collections import defaultdict
from pyspark.sql import SparkSession
from collections import defaultdict

# create a spark session
spark = SparkSession\
    .builder\
    .appName("ques2_part1")\
    .getOrCreate()

df = spark.read.option("header", True).csv("/usr/local/spark/input/shot_logs.csv")

#filter missed records
missed_shot_logs_data = df.filter(df['SHOT_RESULT'] == "missed")

#filter made records
made_shot_logs_data = df.filter(df['SHOT_RESULT'] == "made")

# select all the required columns and drop the null values
missed_shot_logs_data = missed_shot_logs_data.select(missed_shot_logs_data["CLOSEST_DEFENDER"], missed_shot_logs_data["player_name"], missed_shot_logs_data["SHOT_RESULT"]).na.drop()

# select all the required columns and drop the null values
made_shot_logs_data = made_shot_logs_data.select(made_shot_logs_data["CLOSEST_DEFENDER"], made_shot_logs_data["player_name"], made_shot_logs_data["SHOT_RESULT"]).na.drop()

#run map reduce
missed_cnt_shot_logs = missed_shot_logs_data.rdd.map(lambda x: [(x["player_name"], x["CLOSEST_DEFENDER"]), 1]).reduceByKey(add)

#run map reduce
made_cnt_shot_logs = made_shot_logs_data.rdd.map(lambda x: [(x["player_name"], x["CLOSEST_DEFENDER"]), 1]).reduceByKey(add)

#Sort values in descending
shot_logs_count_missed_desc = missed_cnt_shot_logs.sortBy(lambda line: line[0][0], ascending=False)

#Sort values in descending
shot_logs_count_made_desc = made_cnt_shot_logs.sortBy(lambda line: line[0][0], ascending=False)

attackers_most_unwanted_players = defaultdict(lambda: [])

#find the most unwanted layer
missed_dict1 = {}
made_dict2 = {}

#put missed records in a dictionary
for record, missed_shots_temp in shot_logs_count_missed_desc.collect():
    missed_dict1[record] = missed_shots_temp

#put made records in a dictionary
for record, made_shots_temp in shot_logs_count_made_desc.collect():
    made_dict2[record] = made_shots_temp

#find the hit rate for all player-defender combinations
for record in missed_dict1.keys():
    if record in made_dict2:
        attackers_most_unwanted_players[record[0]].append((record[1], (missed_dict1[record] / (missed_dict1[record] + made_dict2[record]))))
    else:
        attackers_most_unwanted_players[record[0]].append((record[1], 1))

for  key in attackers_most_unwanted_players.keys():
    if key == "brian roberts":
        print(attackers_most_unwanted_players[key])

attackers_most_unwanted_player_final = {}

#find the hit rate for a player
for player in attackers_most_unwanted_players.keys():
    for defender_name, hit_rate in attackers_most_unwanted_players[player]:
        if player not in attackers_most_unwanted_player_final:
            attackers_most_unwanted_player_final[player] = (defender_name, hit_rate)
        else:
            # print('In else')
            if hit_rate > attackers_most_unwanted_player_final[player][1]:
                print('In else if')
                attackers_most_unwanted_player_final[player] = (defender_name, hit_rate)

for player in attackers_most_unwanted_players.keys():
    print(player, "'s most unwanted player is ", attackers_most_unwanted_player_final[player][0])

spark.stop()
