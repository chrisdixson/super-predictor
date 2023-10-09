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
# MAGIC This is saved to a spark dataframe at the location: 'dbfs:/FileStore/master_df.csv'

# COMMAND ----------

#### GET ALL SERIES URLS ####

from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
import requests
import re

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

### FOR TESTING INDIVIDUAL HELPER FUNCTIONS ###
# request = requests.get(sample_series)
# soup    = BeautifulSoup(request.text, "html.parser")
# games   = soup.find(class_="ds-mb-4").find_all('div', class_="ds-flex")
# start_date = date(2018, 1, 1)
# end_date = date.today() - timedelta(days=1)

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
        teams  = element.find_all('p', class_= 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
        team_info_list = [teams[0].text, 'N/A', 'N/A', 'N/A', teams[1].text, 'N/A', 'N/A', 'N/A']

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
            result_list = [game_list[i].append(postponed.text), 'N/A']
        else:
            result_list = ['No Result', 'N/A']
    else:
        result_list = ['No Result', 'N/A']
    
    return result_list

#### FOR TESTING INDIVIDUAL HELPER FUNCTIONS ####
# for i, element in enumerate(games):
#     if check_result(element) and check_date(element, start_date, end_date):
#         print(check_result(element), check_date(element, start_date, end_date))
#         result = check_result(element)
#     else:
#         continue
#     print(team_info(element, result))
#     print(get_result(element, result))
        

# COMMAND ----------

# ## FOR TESTING INDIVIDUAL HELPER FUNCTIONS ###
# request = requests.get(sample_series)
# soup    = BeautifulSoup(request.text, "html.parser")
# games   = soup.find(class_="ds-mb-4").find_all('div', class_="ds-flex")
# start_date = date(2018, 1, 1)
# end_date = date.today() - timedelta(days=1)

# # access the series webpage and pick out all the games #

# def team_info(element, result):
    
#     team1_info = element.select('div.ci-team-score.ds-flex.ds-justify-between.ds-items-center.ds-text-typo.ds-my-1')
#     print(team1_info)
#     parsed_divs = [BeautifulSoup(div, 'html.parser') for div in team1_info]
#     for div in parsed_divs:
#         p_element = div.find('p', class_='ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
#         if p_element:
#             print(p_element.text)
#     # teams = team1_info.find('p', class_= 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
#     # print(teams)
#     # team2_info = element.find('div', class_='ci-team-score.ds-flex.ds-justify-between.ds-items-center.ds-text-typo.ds-my-1')

#     # if result == 'RESULT':
#     #     # get team name, runs, wickets and overs information and add it to the game within game_list
#     #     team_info_list = []
#     #     for info in [team1_info, team2_info]:
#     #         team  = info.find('p', class_= 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate').text
#     #         over  = info.find('span', class_ = 'ds-text-compact-xs ds-mr-0.5').text.strip()
#     #         over = over if over else '(50/50 ov)'
#     #         score = info.find('strong', class_ = '').text 
#     #         score += '/10' if '/' not in score else ''
#     #         runs, wickets = score.split('/')
#     #         [team_info_list.append(x) for x in [team, over, runs, wickets]]
#     # else:
#     #     team1  = element.find('p', class_= 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
#     #     team2  = element.find('p', class_ = 'ds-text-tight-m ds-font-bold ds-capitalize ds-truncate')
#     #     team_info_list = [team1.text, 'N/A', 'N/A', 'N/A', team2.text, 'N/A', 'N/A', 'N/A']

#     # return team_info_list

# #### FOR TESTING INDIVIDUAL HELPER FUNCTIONS ####
# for i, element in enumerate(games):
#     if check_result(element) and check_date(element, start_date, end_date):
#         print(check_result(element), check_date(element, start_date, end_date))
#         result = check_result(element)
#     else:
#         continue
#     print(team_info(element, result))
#     print(get_result(element, result))

# COMMAND ----------

#### SCRAPE ALL GAME INFO FROM SERIES URL ####

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
    
    return game_list

# COMMAND ----------

sample_series = 'https://www.espncricinfo.com/series/asia-cup-2018-1153237/match-schedule-fixtures-and-results'
games = game_df(sample_series)
for i, game in enumerate(games):
    print(f'game {i}: {game}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC do not run the below code unless required to pull additional game information.

# COMMAND ----------

import pandas as pd
import requests
from requests.exceptions import RequestException
from datetime import date, datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, DateType

master_df_path    = 'dbfs:/FileStore/master_df.csv'
initial_pull_date = date(2018, 1, 1)
end_date          = date.today() - timedelta(days=1)
series_urls       = get_series_urls(2011)

schema = StructType([StructField('date', DateType(), True),
                     StructField('second_innings_team', StringType(), True), 
                     StructField('second_innings_over_info', StringType(), True),   
                     StructField('second_innings_runs', StringType(), True),
                     StructField('second_innings_wickets', StringType(), True),
                     StructField('first_innings_team', StringType(), True),
                     StructField('first_innings_over_info', StringType(), True),                    
                     StructField('first_innings_runs', StringType(), True),
                     StructField('first_innings_wickets', StringType(), True),
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
                 'url', 'game_name']
master_df = master_df.select(desired_order)

for i, series in enumerate(series_urls):
    try:
        print(f'Success {i}: {series}')
        series_df = spark.createDataFrame(game_df(series), schema = schema)
        series_df = series_df.select(desired_order)
        master_df = master_df.union(series_df)
    except RequestException:
        print(f'Request Exception: could not pull from the following series link: {series}')
        break

# master_df.coalesce(1).write.csv('dbfs:/FileStore/super-cricket/master_df.csv', header = True, mode= "overwrite")

# COMMAND ----------

master_df.coalesce(1).write.csv('dbfs:/FileStore/super-cricket/master_df.csv', header = True, mode= "overwrite")

# COMMAND ----------

master_df = spark.read.csv('dbfs:/FileStore/super-cricket/master_df.csv', header = True, inferSchema = True)
master_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, year

world_cup_teams = ['Afghanistan', 'Australia', 'Bangladesh', 'England', 'India', 
                   'Netherlands', 'New Zealand', 'Pakistan', 'South Africa', 'Sri Lanka']

master_df_filtered = master_df.filter(year(col('date')) >= 2022)
master_df_filtered = master_df_filtered.filter(col('first_innings_team').isin(world_cup_teams) & col('second_innings_team').isin(world_cup_teams))
master_df_filtered.coalesce(1).write.csv('dbfs:/FileStore/super-cricket/master_df_filtered.csv', header = True, mode= "overwrite")

# COMMAND ----------

game_urls = get_all_game_urls(2018)

# COMMAND ----------

print(links[0])
game = get_clean_game(links[0])
for x in game:
    for y in x:
        print(y)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ICC News Articles Scrape
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Graveyard ##

# COMMAND ----------

def clean_over(over):
    over_data = json.loads(over)
    balls = over_data['comments']
    if not balls:
        return ""
    currOver = over_data['comments'][0]['overNumber']
    total_runs, total_wickets = 0, 0
    Output = ""
    for ball in balls:
        #Check if new over
        if ball['overNumber'] != currOver:
            #Output = f"Over {currOver} summary: {total_runs} runs, {total_wickets}\n" + Output
            total_runs, total_wickets, Output = 0, 0, ""
            currOver = ball['overNumber']

        #Adding totals for Over summary
        total_runs += ball['totalRuns']
        if ball['isWicket']:
            total_wickets += 1

        #Ball-by-ball comments
        if ball['commentTextItems']:
            #Only display balls that have corresponding comments
            Output += f"{currOver}.{ball['ballNumber']} "
            Output += ball['commentTextItems'][0]['html']
            Output += '\n'
    Output = f"Over {currOver} summary: {total_runs} runs, {total_wickets} wickets\n" + Output
    return Output

# COMMAND ----------

import pandas as pd

commentary_links = pd.DataFrame(links, columns=["url"])
commentary_links = spark.createDataFrame(commentary_links)
commentary_links.write.saveAsTable("commentary_links")

# COMMAND ----------

ds-text-tight-s ds-font-medium ds-bg-ui-fill-alternate ds-py-1 ds-px-2 ds-rounded-2xl
<div class="ds-text-compact-xs ds-font-bold ds-w-24">Fri, 12 Apr '19</div>


# COMMAND ----------



print(master_df)

# COMMAND ----------

print(game_df('https://www.espncricinfo.com/series/uae-in-zimbabwe-odis-2019-1179270/match-schedule-fixtures-and-results'))

# COMMAND ----------

import re
import requests
from bs4 import BeautifulSoup

url = "https://www.espncricinfo.com/team"
doc = BeautifulSoup(result.text, "html.parser")

links = doc.find_all('a')
team_mappings = {}
counter = 0
for link in links:
    team = re.match("/team/([\w-]+)-(\d+)", link['href'])
    if team:
        if counter < 20:
           team_mappings[team.group(1)] = team.group(2)
        counter += 1

# COMMAND ----------

## Returns the dictionary {game_name: game_url} for every game from a given year onwards ##
def get_all_game_urls(year):
    
    series_urls  = get_series_urls(year)
    url_dict     = {}

    for url in series_urls:
        new_dict = get_game_url(url, commentary = True)
        url_dict.update(new_dict)
    
    return url_dict
