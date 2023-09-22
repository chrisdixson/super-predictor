# Databricks notebook source
pip install bs4

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Load Data

# COMMAND ----------

#Read master_data
master_df = spark.read.csv('dbfs:/FileStore/super-cricket/master_df.csv', header = True, inferSchema = True)
master_df_filtered = spark.read.csv('dbfs:/FileStore/super-cricket/master_df_filtered.csv', header = True, inferSchema = True)
for url in master_df.toPandas()["scorecard_url"]:
    print(url)
master_df.show()
    print(f"https://www.espncricinfo.com{url}")
# asia_cup_df = master_df.filter(master_df["series"] == "asia-cup-2023")
# asia_cup_df.show()

# COMMAND ----------

import re
def result_to_winner(result):
    if "won" not in result:
        return None
    else:
        return result.split("won")[0]

result_series = master_df.toPandas()["result"].apply(result_to_winner)

for result in result_series:
    print(result)
    

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date, timedelta
from pyspark.sql.functions import lit


# replace acronyms
def replace_acronyms(df, column):
    modes = {'b': 'bowled', 'c': 'caught', 'lbw': 'leg before wicket'}
    for i, entry in enumerate(df[column]):
        if entry is not None:
            for key in modes.keys():
                entry = entry.replace(key, modes[key])
            df.at[i, column] = entry

# converts an HTML table to a dataframe -> returns the name and spark df
def tableClass_to_df(table):
    headers = []
    rows = []

    #find name
    i = 0
    name = ""

    # Extract table headers
    for th in table.find_all('th'):
        if re.match("\S+", th.text):
            headers.append(th.text.strip())
            if re.match("BATTING", th.text):
                headers.append("Dismissal")
        if i == 0:
            name = th.text
        i += 1

    # Extract table rows
    for tr in table.find_all('tr'):
        row = []
        for td in tr.find_all('td'):
            if re.match("\S+", td.text):
                row.append(td.text.strip())

        if len(row) == len(headers):
            rows.append(row)
    
    # Set up DataFrame Schema
    schema_list = []
    for header in headers:
        schema_list.append(StructField(str(header), StringType(), True ))
    schema = StructType(schema_list)

    # Create DataFrame
    table_df = spark.createDataFrame(rows, schema = schema)

    return name, table_df

# Returns the schema for batting and bowling dataframes
def schemas():
    
    bat_columns      = ["BATTING", "Dismissal", "R", "B", "M", "4s", "6s", "SR"]
    bowl_columns     = ["BOWLING", "O", "M", "R", "W", "ECON", "0s", "4s", "6s", "WD", "NB"]
    bat_schema_list  = []
    bowl_schema_list = []

    for column in bat_columns:  bat_schema_list.append(StructField(str(column), StringType(), True))
    for column in bowl_columns: bowl_schema_list.append(StructField(str(column), StringType(), True))
    
    bowl_schema_list.append(StructField('country', StringType(), False))
    bowl_schema_list.append(StructField('game', StringType(), False))
    bowl_schema_list.append(StructField('series', StringType(), False))

    bat_schema_list.append(StructField('country', StringType(), False))
    bat_schema_list.append(StructField('game', StringType(), False))
    bat_schema_list.append(StructField('series', StringType(), False))

    bowl_schema = StructType(bowl_schema_list)
    bat_schema  = StructType(bat_schema_list)

    return bowl_schema, bat_schema


def save_scorecard(url, game_name, game_series, game_date):
    
    #make_soup
    url = "https://www.espncricinfo.com" + url
    result = requests.get(url)
    doc = BeautifulSoup(result.text, "html.parser")

    #table = doc.find('table')
    bowl_schema, bat_schema = schemas()

    game_bat  = spark.createDataFrame([], schema = bat_schema)
    game_bowl = spark.createDataFrame([], schema = bowl_schema)

    tables = doc.find_all('table')

    for table in tables:
        
        parent = table.parent
        sib = parent.find_previous_sibling()
        #print(set([x.text for x in sib.find_all('span')]))
        #table_name = "".join(list(set([x.text for x in sib.find_all('span')])))
        if sib:
            table_name = [x.text for x in sib.find_all('span')].pop(0)
            try:
                if team1:
                    team2 = [x.text for x in sib.find_all('span')].pop(0)
            except:
                team1 = [x.text for x in sib.find_all('span')].pop(0)
        else:
            break
            #break gets rid of match details for now and just retrieves batting and bowling
            name = ""
        #print(table_name)
        if table_name == "MATCH DETAILS":
            continue

        #print(table.prettify())
        name, table_df = tableClass_to_df(table)
        # print(f"$$$$$$${table_name}$$$$$$$$")
        # table_df.show()
        if table_name == team1:
            if name == 'BATTING':
                team     = re.sub(r'\s*\([^)]*\)', '', table_name)
                table_df = table_df.withColumn('country', lit(team))
                table_df = table_df.withColumn('game', lit(game_name))
                table_df = table_df.withColumn('series', lit(game_series))
                game_bat = game_bat.union(table_df)
            elif name == 'BOWLING':
                old_bowling = table_df


        else:
            if name == 'BATTING':
                team     = re.sub(r'\s*\([^)]*\)', '', table_name)
                table_df = table_df.withColumn('country', lit(team))
                table_df = table_df.withColumn('game', lit(game_name))
                table_df = table_df.withColumn('series', lit(game_series))
                game_bat = game_bat.union(table_df)
            elif name == 'BOWLING':
                team     = re.sub(r'\s*\([^)]*\)', '', team1)
                table_df = table_df.withColumn('country', lit(team))
                table_df = table_df.withColumn('game', lit(game_name))
                table_df = table_df.withColumn('series', lit(game_series))
                game_bowl = game_bowl.union(table_df)
                team     = re.sub(r'\s*\([^)]*\)', '', team2)
                old_bowling = old_bowling.withColumn('country', lit(team))
                old_bowling = old_bowling.withColumn('game', lit(game_name))
                old_bowling = old_bowling.withColumn('series', lit(game_series))
                game_bowl = game_bowl.union(old_bowling)


        # if name == 'BATTING':
        #     game_bat = game_bat.union(table_df)
        # elif name == 'BOWLING':
        #     game_bowl = game_bowl.union(table_df)
        # else:
        #     continue

        #game_bat.show(100)

        table_df.coalesce(1).write.csv(f'dbfs:/FileStore/master_df/scorecard_data/{table_name}.csv', header = True, mode= "overwrite")
    # game_bat.show()
    # game_bowl.show()
    return name, game_bat, game_bowl

# name, game_bat, game_bowl = save_scorecard("/series/aus-in-eng-2018-1119525/england-vs-australia-1st-odi-1119537/full-scorecard",
#                                             "scotland-vs-engla",
#                                             "2018-06-10")

def save_master_df_scorecards(df):
    
    bowl_schema, bat_schema = schemas()

    master_bat  = spark.createDataFrame([], schema = bat_schema)
    master_bowl = spark.createDataFrame([], schema = bowl_schema)
    
    rows = df.collect()
    for row in rows:
        try:
            name, game_bat, game_bowl = save_scorecard(row["scorecard_url"], row["game_name"], row["series"],row["date"])
            master_bat = master_bat.union(game_bat)
            master_bowl= master_bowl.union(game_bowl)
        except UnboundLocalError:
            print('No Result')
    return master_bat, master_bowl

# COMMAND ----------

master_bat_filtered, master_bowl_filtered = save_master_df_scorecards(master_df_filtered)

# COMMAND ----------

display(master_bowl_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Clean Data

# COMMAND ----------

from pyspark.sql.functions import udf, col

# Remove special characters
def remove_special_chars(text):
    return ''.join([char for char in text if ord(char) < 127])

remove_special_chars_udf = udf(remove_special_chars, StringType())

master_bat_filtered = master_bat_filtered.withColumn("BATTING", remove_special_chars_udf(col("BATTING")))
master_bat_filtered = master_bat_filtered.withColumn("Dismissal", remove_special_chars_udf(col("Dismissal")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Store Data

# COMMAND ----------

root = 'dbfs:/FileStore/super-cricket/'

master_bat_filtered.coalesce(1).write.csv(f'{root}/master_bat_filtered_series.csv', header = True, mode= "overwrite")
master_bowl_filtered.coalesce(1).write.csv(f'{root}/master_bowl_filtered_series.csv', header = True, mode= "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Graveyard / Playground

# COMMAND ----------

dismissals = master_bat.select('Dismissal').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

string = 'c Aasif Sheikh b Karan KC'
string = 'run out (Paudel)'
dismissal_list = string.split(' b ')

dismissal_bowler = []

for dismissal in dismissals:
    dismissal_list = dismissal.split(' b ')
    if len(dismissal_list) == 2:
        

print(dismissal_list)
print(dismissals)

# COMMAND ----------

master_bowl = spark.read.csv('dbfs:/FileStore/super-cricket/master_bat_filtered_series.csv', header = True, inferSchema = True)

# COMMAND ----------

display(master_bowl)