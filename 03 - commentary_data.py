# Databricks notebook source
master_df = spark.read.csv('/dbfs/FileStore/master_df.csv', header = True, inferSchema = True)

# COMMAND ----------

import re
import requests
import json

## Input game url, innings number and over number -> outputs dictionary of raw ball by ball information ##
def get_raw_over(url, innings, inningsOver):
    
    # grab matchId and seriesId
    url_match = re.match("https://www.espncricinfo.com/series/([\w\d-]+)-(\d+)/([\w\d-]+)-(\d+)/ball-by-ball-commentary", url)
    seriesId, matchId = url_match.group(2), url_match.group(4)

    # GET text
    request = f"https://hs-consumer-api.espncricinfo.com/v1/pages/match/comments?seriesId={seriesId}&matchId={matchId}&inningNumber={innings}&commentType=ALL&fromInningOver={inningsOver}"
    payload, headers = {}, {}  
    response         = requests.request("GET", request, headers=headers, data=payload)
    raw_over         = json.loads(response.text)

    return raw_over["comments"][0]

## Cleans an over of commentary data -> outputs string of ball by ball info for the over ##
def clean_over(over):
    
    # Initial clean + reverse order
    over  = over['comments']
    Output = ''
    if not over: return Output
    current_over = over[0]['overNumber']
    over.reverse()
    
    # Determine who is batting and save as first line of output 
    if current_over == 1:
        batting_team = over[-1]['over']['team']['longName']
        Output      += f'{batting_team} Innings: \n \n'

    for i in range(len(over)):  
        
        # Remove double erroneous previous overs from raw data
        if current_over != over[i]['overNumber']: continue

        # ball information
        ball_number  = over[i]['ballNumber']
        bowl_to_bat  = over[i]['title']
        event        = ''
        runs         = over[i]['totalRuns']

        # Special events such as wickets, fours and sixes 
        event_dict = {'isFour': 'Four!', 'isSix': 'Six!', 'isWicket': 'Wicket!', 'legbyes': 'legbyes', 'wides': 'Wide.', 'noballs': 'No Ball.'}
        for key, item in event_dict.items():
            if over[i][key]:
                event = item

        Output += f'{current_over}.{ball_number} {bowl_to_bat}: {event} {runs} run(s) \n'

        # Commentary prior to, during and after ball #
        comment_elements = ['commentPreTextItems', 'commentTextItems', 'commentPostTextItems']
        comments         = ''
        for x in comment_elements:
            try:
                comments += f"{over[i][x][0]['html']}. "
            except TypeError:
                continue
        if comments: 
            Output += f'{comments} \n'

    return Output 


## Input url -> outputs list of game commentary ##
def get_game(url, clean = True):

    game = []

    for innings_number in range(1,3):
        for over in range(1,51):
            over   = get_raw_over(url, innings_number, over)
            if clean:
                over   = clean_over(raw_over)
            game.append(over)

    return game

# COMMAND ----------

url_list   = master_df.select("ball_by_ball_commentary_url").rdd.flatMap(lambda x: x).collect()
sample_url = url_list[0]

# COMMAND ----------

raw_game   = get_game(sample_url, clean = False)
first_over = get_raw_over(sample_url,1,1)
print(first_over)

# COMMAND ----------

voer_df = spark.createDataFrame(first_over.items(), ['key', 'value'])
voer_df.show()