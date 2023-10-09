# Databricks notebook source
# MAGIC %pip install bs4

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Section 1 - Get master data: urls and results for past, present and scheduled games
# MAGIC
# MAGIC the following section scrapes the website for all ODI series from the following link: https://www.espncricinfo.com/records/list-of-series-results-335432 
# MAGIC
# MAGIC Information output for each series includes: ['date', 'team1', 'result', 'scorecard_url', 'ball_by_ball_commentary_url', 'series', 'game_name', 'team2']
# MAGIC
# MAGIC This is saved to a spark dataframe at the location: 'dbfs:/FileStore/master_df.parquet'

# COMMAND ----------

#### GET ALL SERIES URLS ####

from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
import requests
import re
import os

## Returns a list of urls for all series from a given year onwards ##
def get_series_urls(start_year):
    
    # access the webpage and pick out table of series #
    series_list_url = "https://www.espncricinfo.com/records/list-of-series-results-335432"
    odi_request     = requests.get(series_list_url)
    soup            = BeautifulSoup(odi_request.text, "html.parser")
    table           = soup.find("table")
    rows            = table.find_all("tr")
    series_urls     = []

    # pick each series in the table of series #
    for series in rows:
        if series.parent.name == "thead":
            continue
        series_data = series.find_all("td")
        
        # pick out the year and url for the series #
        for i, data in enumerate(series_data):
            if i == 0:
                link = data.find("a")
            elif i == 1:
                year       = data.find("span").text
                year_start = re.match("(\d{4}).*", year)
                if int(year_start.group(1)) >= start_year:
                    series_urls.append("https://www.espncricinfo.com"+link["href"])
    return series_urls

# COMMAND ----------

# access the series webpage and pick out all the games #

#### HELPER FUNCTIONS ####

def check_result(element):
    
    RESULT = element.find('span', class_ = 'ds-text-tight-xs ds-font-bold ds-uppercase ds-leading-5')
    
    if RESULT:
        RESULT = RESULT.text

    return RESULT

def check_date(element, start_date, end_date):
    # get date and add it to the game_list #
    game_date_raw = element.find('div', class_="ds-text-compact-xs ds-font-bold ds-w-24")
    raw_format    = r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '\d{2}$"


    if game_date_raw and re.match(raw_format, game_date_raw.text):
        game_date = datetime.strptime(game_date_raw.text, "%a, %d %b '%y").date()
        if game_date < start_date or game_date > end_date:
            return False
    elif game_date_raw:
        return "Empty"
    else:
        return False
    return game_date

def team_info(element, result):
    
    teams_info = element.find_all('div', class_=re.compile("^ci-team-score"))
 
    if result == 'RESULT':
        # get team name, runs, wickets and overs information and add it to the game within game_list
        team_info_list = []
        for info in teams_info:
            team  = info.find('p', class_= 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate').text
            over  = info.find('span', class_ = 'ds-text-compact-xs ds-mr-0.5').text.strip()
            over = over if over else '(50/50 ov)'
            score = info.find('strong', class_ = '').text 
            score += '/10' if '/' not in score else ''
            runs, wickets = score.split('/')
            [team_info_list.append(x) for x in [team, over, runs, wickets]]
    else:
        team1  = element.find('p', class_= 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
        team2  = element.find('p', class_ = 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
        team_info_list = [team1.text, 'N/A', 'N/A', 'N/A', team2.text, 'N/A', 'N/A', 'N/A']

    #print(team_info_list)
    return team_info_list

def get_result(element, result):
    # get game result and winner and add it to the game within game_list #
    if result == "RESULT":
        game_result = element.find('p', class_ = 'ds-text-tight-s ds-font-regular ds-line-clamp-2 ds-text-typo')
        postponed = element.find('span', class_ = 'ds-text-tight-xs ds-font-bold ds-uppercase ds-leading-5')
        if game_result: 
            split = game_result.text.split(' won ')
            if len(split) == 1:
                result_list = [game_result.text, 'match tied']
            else:
                result_list = [game_result.text, split[0]]
        elif postponed.text == 'POSTPONED': 
            result_list = [postponed.text, 'N/A']
        else:
            result_list = ['No Result', 'N/A']
    else:
        result_list = ['No Result', 'N/A']
    
    return result_list

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

#### SCRAPE ALL GAME INFO FROM SERIES URL ####

def game_df(series_url, start_date = date(2011, 1, 1), end_date = date.today() - timedelta(days=1)):

    # access the series webpage and pick out all the games #
    request = requests.get(series_url)
    soup    = BeautifulSoup(request.text, "html.parser")
    games   = soup.find(class_="ds-mb-4").find_all('div', class_="ds-flex")
    #rint(games)
    #games   = soup.find(class_="ds-mb-4").find_all("a")
    #print(games)
    # set helper variables #
    game_list = []
    i = 0

    # get date, and team names for the game #
    for element in games:
        game_date = check_date(element, start_date, end_date)
        try:
            if game_date == "Empty" and last_date:
                game_date = last_date
        except UnboundLocalError:
            continue
        # Check if result or no result and valid date: append the date to the list
        if check_result(element) and game_date:
            last_date = game_date
            result = check_result(element)
            print('Success', game_date, result)
            game_list.append([game_date])
        else: continue
        
        # Append the team_name, over_info, runs, wickets for both teams
        [game_list[i].append(x) for x in team_info(element, result)]
        
        # Append the result summary and winner
        [game_list[i].append(x) for x in get_result(element, result)]

        # get game url and add it to the game within game_list #
        url = element.find('a', class_ = 'ds-no-tap-higlight')
        if url: 
            game_list[i].append(url['href'])

            # get ball by ball commentary url and add it to the game within game_list #
            commentary_url  = "https://www.espncricinfo.com"+url['href'].replace("full-scorecard", "ball-by-ball-commentary")
            game_list[i].append(commentary_url)

            # get series name and game name and add them to the game within game_list #
            url_match = re.match("^/series/([\w\d-]+)-\d+/([\w\d-]+)-\d+/full-scorecard$", url['href'])
            if url_match:
                series_name = url_match.group(1)
                game_name   = url_match.group(2)
                game_list[i].append(series_name)
                game_list[i].append(game_name)
        i += 1

    # For series with multiple games on a single day populate all dates for that day #
    for j in range(len(game_list)):
        if not game_list[j][0]:
            game_list[j][0] = game_list[j-1][0]
    #print(game_list)
    return game_list

# COMMAND ----------

import pandas as pd
import requests
from requests.exceptions import RequestException
from datetime import date, datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, DateType

master_df_path    = 'dbfs:/FileStore/super-cricket/master_df.parquet'
initial_pull_date = date(2021, 1, 1)
end_date          = date.today()
#series_urls       = get_series_urls(2011) #uncomment to run from custom year
series_urls       = get_series_urls(datetime.now().year)

schema = StructType([StructField('date', DateType(), True),
                     StructField('first_innings_team', StringType(), True),
                     StructField('first_innings_over_info', StringType(), True),                    
                     StructField('first_innings_runs', StringType(), True),
                     StructField('first_innings_wickets', StringType(), True),
                     StructField('second_innings_team', StringType(), True), 
                     StructField('second_innings_over_info', StringType(), True),   
                     StructField('second_innings_runs', StringType(), True),
                     StructField('second_innings_wickets', StringType(), True),
                     StructField('result', StringType(), True),
                     StructField('winner', StringType(), True),
                     StructField('scorecard_url', StringType(), True),
                     StructField('ball_by_ball_commentary_url', StringType(), True), 
                     StructField('series', StringType(), True),
                     StructField('game_name', StringType(), True)])

master_df = spark.createDataFrame([], schema=schema) # this line is commented out as the df is already saved to dbfs - uncomment to clear and     reupload to the db
desired_order = ['date', 'first_innings_team', 'first_innings_over_info', 'first_innings_runs',
                 'first_innings_wickets', 'second_innings_team', 'second_innings_over_info', 'second_innings_runs',
                 'second_innings_wickets', 'result', 'winner', 'scorecard_url', 'ball_by_ball_commentary_url',
                 'series', 'game_name']
# master_df = master_df.select(desired_order)

for i, series in enumerate(series_urls):
    try:
        print(f'Success {i}: {series}')
        series_df = spark.createDataFrame(game_df(series, end_date = end_date), schema = schema)
        series_df = series_df.select(desired_order)
        #print(series_df)
        master_df = master_df.union(series_df)
    except RequestException:
        print(f'Request Exception: could not pull from the following series link: {series}')
        break

# COMMAND ----------

#Remove all games that are already in dataset
overwrite =  False #Set to true to recalculate whole file
if file_exists(master_df_path) and not overwrite:
    existing_df = spark.read.parquet(master_df_path)
    master_df_delta = master_df.exceptAll(existing_df)
    master_df_delta.coalesce(1).write.parquet(master_df_path, mode= "append")
    master_df = spark.read.parquet(master_df_path)
else:
    master_df.coalesce(1).write.parquet(master_df_path, mode= "overwrite")

# COMMAND ----------

from pyspark.sql.functions import col

world_cup_teams = ['Afghanistan', 'Australia', 'Bangladesh', 'England', 'India', 
                   'Netherlands', 'New Zealand', 'Pakistan', 'South Africa', 'Sri Lanka']
master_df_filtered = master_df.filter(col('first_innings_team').isin(world_cup_teams) & col('second_innings_team').isin(world_cup_teams))

#Only append rows which are not contained in the existing file
filtered_filename = 'dbfs:/FileStore/super-cricket/master_df_filtered.parquet'
if file_exists(filtered_filename) and not overwrite:
    existing_filtered_df = spark.read.parquet(filtered_filename)
    filtered_delta_df = master_df_filtered.exceptAll(existing_filtered_df)
    filtered_delta_df.write.parquet(filtered_filename, mode= "append")
    master_df_filtered = spark.read.parquet(filtered_filename)
else:
    master_df_filtered.write.parquet(filtered_filename, mode= "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #Get Batting and Bowling Data

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
    bowl_schema_list.append(StructField('match_ground', StringType(), False))

    bat_schema_list.append(StructField('country', StringType(), False))
    bat_schema_list.append(StructField('game', StringType(), False))
    bat_schema_list.append(StructField('series', StringType(), False))
    bat_schema_list.append(StructField('match_ground', StringType(), False))

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

    table_dfs = dict()

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
            #update match_ground in game_bat and game_bowl
            match_ground = table.find('tr').find('span').text
            game_bat = game_bat.withColumn('match_ground', lit(match_ground))
            game_bowl = game_bowl.withColumn('match_ground', lit(match_ground))

            #update in csv dict table_dfs
            for table_name, table_df in table_dfs.items(): 
                table_df.withColumn('match_ground', lit(match_ground))
            continue


        name, table_df = tableClass_to_df(table)
        if table_name == team1:
            if name == 'BATTING':
                team     = re.sub(r'\s*\([^)]*\)', '', table_name)
                csv_name = team+'_BOWLING_'+game_name+'_'+game_series+'.csv'
                file_path = f'dbfs:/FileStore/master_df/scorecard_data/{csv_name}.parquet'
                if os.path.exists(file_path):
                    table_df = spark.read.parquet(file_path)
                else:
                    table_df = table_df.withColumn('country', lit(team))
                    table_df = table_df.withColumn('game', lit(game_name))
                    table_df = table_df.withColumn('series', lit(game_series))
                    table_df = table_df.withColumn('match_ground', lit('init'))
                    table_dfs[csv_name] = table_df
                game_bat = game_bat.union(table_df)
            elif name == 'BOWLING':
                old_bowling = table_df


        else:
            if name == 'BATTING':
                team     = re.sub(r'\s*\([^)]*\)', '', table_name)
                csv_name = team+'_BOWLING_'+game_name+'_'+game_series+'.csv'
                file_path = f'dbfs:/FileStore/master_df/scorecard_data/{csv_name}.parquet'
                if os.path.exists(file_path):
                    table_df = spark.read.parquet(file_path)
                else:
                    table_df = table_df.withColumn('country', lit(team))
                    table_df = table_df.withColumn('game', lit(game_name))
                    table_df = table_df.withColumn('series', lit(game_series))
                    table_df = table_df.withColumn('match_ground', lit('init'))
                    table_dfs[csv_name] = table_df
                game_bat = game_bat.union(table_df)
                
            elif name == 'BOWLING':
                team     = re.sub(r'\s*\([^)]*\)', '', team1)
                csv_name = team+'_BOWLING_'+game_name+'_'+game_series+'.csv'
                file_path = f'dbfs:/FileStore/master_df/scorecard_data/{csv_name}.parquet'
                if os.path.exists(file_path):
                    table_df = spark.read.parquet(file_path)
                else:
                    table_df = table_df.withColumn('country', lit(team))
                    table_df = table_df.withColumn('game', lit(game_name))
                    table_df = table_df.withColumn('series', lit(game_series))
                    table_df = table_df.withColumn('match_ground', lit('init'))
                    table_dfs[csv_name] = table_df
                game_bowl = game_bowl.union(table_df)

                team     = re.sub(r'\s*\([^)]*\)', '', team2)
                csv_name = team+'_BOWLING_'+game_name+'_'+game_series+'.csv'
                file_path = f'dbfs:/FileStore/master_df/scorecard_data/{csv_name}.parquet'
                if os.path.exists(file_path):
                    table_df = spark.read.parquet(file_path)
                else:
                    old_bowling = old_bowling.withColumn('country', lit(team))
                    old_bowling = old_bowling.withColumn('game', lit(game_name))
                    old_bowling = old_bowling.withColumn('series', lit(game_series))
                    old_bowling = old_bowling.withColumn('match_ground', lit('init'))
                    table_dfs[csv_name] = old_bowling
                game_bowl = game_bowl.union(old_bowling)

    #for table_name, table_df in table_dfs.items():
    #    table_df.coalesce(1).write.parquet(f'dbfs:/FileStore/master_df/scorecard_data/{table_name}.parquet', mode= "overwrite")
    return name, game_bat, game_bowl

def save_master_df_scorecards(df, overwrite = False):
    
    bowl_schema, bat_schema = schemas()

    master_bat  = spark.createDataFrame([], schema = bat_schema)
    master_bowl = spark.createDataFrame([], schema = bowl_schema)

    if not overwrite:
        bat_file_path = 'dbfs:/FileStore/super-cricket/master_bat_filtered_series.parquet'
        bowl_file_path = 'dbfs:/FileStore/super-cricket/master_bowl_filtered_series.parquet'
        if file_exists(bat_file_path):
            master_bat = spark.read.parquet(bat_file_path)
        if file_exists(bowl_file_path):
            master_bowl = spark.read.parquet(bowl_file_path)
    
    rows = df.collect()
    for row in rows:
        try:
            series = master_bat.filter(col("series").contains(row["series"]))
            if (series.count() == 0) or (series.filter(col("game").contains(row["game_name"])).count() == 0):
                print(f'Game not saved {row["game_name"]} {row["series"]}')
                name, game_bat, game_bowl = save_scorecard(row["scorecard_url"], row["game_name"], row["series"],row["date"])
                master_bat = master_bat.union(game_bat)
                master_bowl= master_bowl.union(game_bowl)
        except UnboundLocalError:
            print('No Result')
    return master_bat, master_bowl

# COMMAND ----------

master_bat_filtered, master_bowl_filtered = save_master_df_scorecards(master_df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Clean Data

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
# MAGIC ##Store Data

# COMMAND ----------

root = 'dbfs:/FileStore/super-cricket/'
#Use delta logic
bat_filename = f'{root}/master_bat_filtered_series.parquet'
overwrite = False
if file_exists(bat_filename) and not overwrite:
    existing_bat = spark.read.parquet(bat_filename)
    master_bat_delta = master_bat_filtered.exceptAll(existing_bat)
    master_bat_delta.write.parquet(f'{root}/master_bat_filtered_series.parquet', mode= "append")
else:
    master_bat_filtered.write.parquet(f'{root}/master_bat_filtered_series.parquet', mode= "overwrite")

bowl_filename = f'{root}/master_bowl_filtered_series.parquet'
if file_exists(bowl_filename) and not overwrite:
    existing_bowl = spark.read.parquet(bowl_filename)
    master_bowl_delta = master_bowl_filtered.exceptAll(existing_bowl)
    master_bowl_delta.write.parquet(f'{root}/master_bowl_filtered_series.parquet', mode= "append")
else:
    master_bowl_filtered.write.parquet(f'{root}/master_bowl_filtered_series.parquet', mode= "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #Update Database tables

# COMMAND ----------

# Function to read data from files and write to table
root = 'dbfs:/FileStore/super-cricket/'

def write_to_table(filenames, tables, overwrite=False):
    for filename, table in list(zip(filenames, tables)):
        file_path = f'{root}/{filename}'
        print(f"Writing file: {file_path}")
        df = spark.read.parquet(file_path)

        table_name = f'super_predictor_v2.{table}'
        table_df = spark.read.table(table_name)
        if overwrite or table_df.count() == 0:
            df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
        else:
            df_delta = df.exceptAll(table_df)
            df_delta.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)

# COMMAND ----------

#Write data to tables

filenames = ["master_bowl_filtered_series.parquet",
             "master_bat_filtered_series.parquet",
             "master_df_filtered.parquet"]

tables = ["master_bowl",
          "master_bat",
          "master_results"]

write_to_table(filenames, tables)
