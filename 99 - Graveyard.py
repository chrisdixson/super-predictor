# Databricks notebook source


# COMMAND ----------




## Returns the list of lists: series = [[game1], [game2]] where game1 = [date, team1, url, etc] from a start_date up to the current_date but  ## ## not including the current day ##

def game_df(series_url, start_date = date(2018, 1, 1), end_date = date.today() - timedelta(days=1)):
    
    # access the series webpage and pick out all the games #
    request = requests.get(series_url)
    soup    = BeautifulSoup(request.text, "html.parser")
    games   = soup.find(class_="ds-mb-4").find_all('div', class_="ds-flex")
    
    # set helper variables #
    game_list = []
    i = 0

    # get date, and team names for the game #
    for element in games:

        # get date and add it to the game_list #
        game_date_raw = element.find('div', class_="ds-text-compact-xs ds-font-bold ds-w-24")
        raw_format    = r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) '\d{2}$"
        if game_date_raw and re.match(raw_format, game_date_raw.text):
            game_date = datetime.strptime(game_date_raw.text, "%a, %d %b '%y").date()
            if start_date < game_date < end_date:
                game_list.append([game_date])
            else:
                continue
        else:
            continue

        # get team names and add them to the game within game_list #
        teams = element.find_all('p', class_= 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
        for team in teams:
            if team and team.text not in game_list[i]: 
                game_list[i].append(team.text)
            else:
                continue
        
        # get score information
        score_info = element.find_all('div', class_='ds-text-compact-s ds-text-typo ds-text-right ds-whitespace-nowrap')
        for k, score in enumerate(score_info):
            over = element.find('span', class_='ds-text-compact-xs ds-mr-0.5')
            print(f'innings {k}: {over.text}')
            score = element.find('strong', class_='')
            print(score.text)


        # get game result and add it to the game within game_list #
        result = element.find('p', class_ = 'ds-text-tight-s ds-font-regular ds-line-clamp-2 ds-text-typo')
        postponed = element.find('span', class_ = 'ds-text-tight-xs ds-font-bold ds-uppercase ds-leading-5')
        if result: game_list[i].append(result.text)
        elif postponed.text == 'POSTPONED': game_list[i].append(postponed.text)
        else: continue 

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
    
    return game_list

# COMMAND ----------

import pandas as pd
import requests
from requests.exceptions import RequestException
from datetime import date, datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, DateType

master_df_path    = 'dbfs:/FileStore/master_df.csv'
initial_pull_date = date(2018, 1, 1)
start_date        = date.today() - timedelta(days=1)
series_urls       = get_series_urls(2022)

schema = StructType([StructField('date', DateType(), True),
                     StructField('first_innings_team', StringType(), True), 
                     StructField('first_innings_runs', StringType(), True),
                     StructField('first_innings_wickets', StringType(), True),
                     StructField('second_innings_team', StringType(), True), 
                     StructField('second_innings_runs', StringType(), True),
                     StructField('second_innings_wickets', StringType(), True),
                     StructField('result', StringType(), True),
                     StructField('winner', StringType(), True),
                     StructField('scorecard_url', StringType(), True),
                     StructField('ball_by_ball_commentary_url', StringType(), True), 
                     StructField('series', StringType(), True),
                     StructField('game_name', StringType(), True)])

# master_df = spark.createDataFrame([], schema=schema) # this line is commented out as the df is already saved to dbfs - uncomment to clear and     reupload to the db

master_df = spark.read.csv(master_df_path, header = True, inferSchema = True)
# # master_df.show()

for i, series in enumerate(series_urls):
    try:
        print(f'Success {i}: {series}')
        series_df = spark.createDataFrame(game_df(series, start_date), schema = schema)
        master_df = master_df.union(series_df)
    except RequestException:
        print(f'Request Exception: could not pull from the following series link: {series}')
        break

master_df.coalesce(1).write.csv('dbfs:/FileStore/super-cricket/master_df.csv', header = True, mode= "overwrite")


# COMMAND ----------

unique_teams = master_df.select("team1").distinct()

unique_teams.tail(50)

# COMMAND ----------

for key, value in game_urls.items():
    game_name = key
    game_url  = value
    game      = get_clean_game(game_url)
    dbutils.fs.put(f"/mnt/blob/commentary_data/{game_name}.txt", game[0])

# COMMAND ----------

with open(f"/dbfs/mnt/blob/{game_name}.txt", "r") as f:
    for line in f:
        print(line)

# COMMAND ----------

dbutils.fs.ls('mnt/blob/commentary_data')