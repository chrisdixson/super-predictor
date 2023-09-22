# Databricks notebook source
# MAGIC %pip install openai tiktoken pinecone-client langchain tqdm

# COMMAND ----------

# imports
import os
import pandas as pd
import pyspark.pandas as ps
from tqdm.notebook import tqdm
from pyspark.sql.functions import date_format
import tiktoken

import openai
from openai.embeddings_utils import get_embedding
from langchain.document_loaders import PySparkDataFrameLoader
from langchain.document_loaders.csv_loader import CSVLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores import Pinecone
import pinecone

# COMMAND ----------

api_key = 'ae014f13-032b-4ba8-986f-465c734e9826'
environment = 'gcp-starter'
# pinecone.init(api_key=api_key, environment=environment)
pinecone.init(
	api_key=api_key,
	environment=environment
)

# COMMAND ----------

# check if 'openai' index already exists (only create index if not)
if 'super-predictor' not in pinecone.list_indexes():
    pinecone.create_index('super-predictor', dimension=1536)
# connect to index
index = pinecone.Index('super-predictor')

# COMMAND ----------

# Filename read prep
def replace_dbfs_prefix(input_string):
    # Check if the string starts with 'dbfs:/'
    if input_string.startswith('dbfs:/'):
        # Replace 'dbfs:/' with '/dbfs/'
        output_string = input_string.replace('dbfs:/', '/dbfs/')
    else:
        # If it doesn't start with 'dbfs:/', keep the original string
        output_string = input_string
    return output_string

# COMMAND ----------

# MAGIC %md
# MAGIC #  Master DF All Text Column
# MAGIC Text preprocessing for faster and more effective LLM queries

# COMMAND ----------

master_df_filtered = spark.read.csv('dbfs:/FileStore/super-cricket/master_df_filtered.csv', header = True, inferSchema = True).pandas_api()
master_df_filtered.head(2)

# COMMAND ----------

master_bowl = spark.read.csv('dbfs:/FileStore/super-cricket/master_bowl_filtered_series.csv', header = True, inferSchema = True).pandas_api()
master_bowl = master_bowl.rename(columns={"O":"overs", "M":"maiden_overs", "R":"runs", "W":"wickets", "ECON":"economy", "WD":"wide", "NB":"no_ball"})
master_bowl.head(2)

# COMMAND ----------

master_bat = spark.read.csv('dbfs:/FileStore/super-cricket/master_bat_filtered_series.csv', header = True, inferSchema = True).pandas_api()
master_bat = master_bat.rename(columns={"O":"overs","R":"runs", "SR":"strike_rate"}).drop(columns=['B', 'M'])
master_bat.head(2)

# COMMAND ----------

#### String Processing

def process_df_to_string(input_df):
    # Get unique values for 'game' and 'series' columns
    unique_values = input_df[['game', 'series']].drop_duplicates()
    
    # Initialize an empty list to store the resulting strings
    result_strings = []

    # Specify the columns to exclude
    columns_to_exclude = ['game', 'series']

    # Iterate through the rows of the DataFrame
    for index, row in tqdm(unique_values.iterrows(), total=len(unique_values), desc="Processing Rows"):
        # Filter Input DataFrame
        df_filter = input_df[(input_df['game'] == row['game']) & (input_df['series'] == row['series'])]

        df_string = ''
        for index_filter, row_filter in df_filter.iterrows():
            # Create a list of "column_name: value" pairs with semicolon separator, excluding specified columns
            column_value_pairs = [f'{column}:{value}' for column, value in row_filter.items() if column not in columns_to_exclude]

            # Join the pairs into a single string with semicolon-separated values
            row_string = '; '.join(column_value_pairs)
            df_string = df_string + '\n' + row_string

        # Append the row string to the result_strings list
        result_strings.append(df_string)

    return result_strings, unique_values

# COMMAND ----------

# Initiate function
result_strings, unique_values = process_df_to_string(master_bowl)

unique_values['bowler_information'] = result_strings
master_bowl_string = unique_values.copy()
master_bowl_string.head(2)

# COMMAND ----------

# Initiate function
result_strings, unique_values = process_df_to_string(master_bat)

unique_values['batter_information'] = result_strings
master_bat_string = unique_values.copy()
master_bat_string.head(2)

# COMMAND ----------

### Join with master
master_df_filtered = master_df_filtered.rename(columns={'game_name':'game'})
# Perform a left join between master_df_filtered and master_bowl_string
result_df = master_df_filtered.merge(master_bowl_string, on=['game', 'series'], how='left')

# Perform a left join between result_df and master_bat_string
final_df = result_df.merge(master_bat_string, on=['game', 'series'], how='left')
final_df.head(2)

# COMMAND ----------

final_df.shape

# COMMAND ----------

#### Alter prompt to consider bat and bowl strings too

openai.api_key = "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"

# Specify the columns to exclude
columns_to_exclude = ['scorecard_url', 'ball_by_ball_commentary_url', 'all_data_string_bowl', 'all_data_string_bat']

# Initialize an empty list to store the resulting strings
result_strings = []

# Iterate through the rows of the DataFrame
for index, row in tqdm(final_df.iterrows(), total=len(final_df), desc="Processing Rows"):
    # Create a list of "column_name: value" pairs with semicolon separator, excluding specified columns
    column_value_pairs = [f'{column}:{value}' for column, value in row.items() if column not in columns_to_exclude]
    
    # Join the pairs into a single string with semicolon-separated values
    row_string = '; '.join(column_value_pairs)

    # response = openai.ChatCompletion.create(
    #     model="gpt-3.5-turbo",
    #     messages=[
    #         {"role": "user", "content": f"You are a professional cricket commentator. Commentate only on the provided information: {row_string}\n With this information in mind, create a another paragraph from the following information: {row['all_data_string_bat']}\n and likewise from the following information: {row['all_data_string_bowl']}"}
    #     ]
    # )
    
    # text_result = response['choices'][0]['message']['content']

    text_result = row_string

    # Append the row string to the result_strings list
    result_strings.append(text_result)


result_strings

# COMMAND ----------

# Master Final
final_df['overall_match_information'] = result_strings
final_df_all = final_df.copy()
final_df_all.head(2)

# COMMAND ----------

#### GO BACK TO ORIGINAL CSV METHOD - BREAK DOWN QUERY BY CODE PROCESSING DO ALL IN DATABRICKS UNDERSTAND HOW DIFFERENT PIECES OF INFORMATINO ARE PRIORITISED IN QUERYING

display(final_df_all)

# COMMAND ----------

final_df_all_select = final_df_all[['date', 'all_data_string_bowl', 'all_data_string_bat', 'all_data_string_final']]

# Replace None values with a string value
string_value = "Data unavailable"
final_df_all_select = final_df_all_select.fillna(string_value)
final_df_all_select = final_df_all_select.to_spark()
display(final_df_all_select)

# COMMAND ----------

### Save new generated data
root = 'dbfs:/FileStore/super-cricket/'

final_df_all_select.write.parquet(f'{root}/master_all_data_v2.parquet')

# COMMAND ----------

display(spark.read.parquet('dbfs:/FileStore/super-cricket/master_all_data_v2.parquet', header = True, inferSchema = True))

# COMMAND ----------

loader_master = PySparkDataFrameLoader(spark, final_df_all_select, page_content_column="all_data_string_final")
documents_master = loader_master.load()

# COMMAND ----------

text_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=0)
master_texts = text_splitter.split_documents(documents_master)

# COMMAND ----------

text_splitter = RecursiveCharacterTextSplitter(
    chunk_size = 1200,
    chunk_overlap  = 200
)
docs_chunks = text_splitter.split_documents(documents_master)
print(docs_chunks)

# COMMAND ----------

os.environ['OPENAI_API_KEY'] = "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"

index_name = "super-predictor"
embeddings = OpenAIEmbeddings(chunk_size = 1)

# create embeddings and store in vectordb
vectordb = Pinecone.from_documents(
  docs_chunks,
  embedding = embeddings,
  index_name = index_name
)

# COMMAND ----------

testingIndex = Pinecone.from_existing_index(index_name, embeddings)

# COMMAND ----------

query = "Who was the highest wicket taker in 2022?"
testingIndex.similarity_search(
    query,  # our search query
    k=3  # return 3 most relevant docs
)

# COMMAND ----------

master_texts

# COMMAND ----------

{"role": "system", "content": "You are a professional cricket commentator. Also make the following assmptions:\n - write in past tense\n - don't make any assumptions unless explicitly mentioned in the list above\n - first innings team refers to the team that first batted"},
            {"role": "user", "content": f"Do not mention the toss and take the following information and create a concise commentary that includes all the information from the following: {row_string}\n Do not mention the toss create an additional paragraph from the following string about the batters of the above described game from the following: {row['all_data_string_bat']}\n Do not mention the toss and create an additional paragraph from the following about the bowlers of the above described game from the following: {row['all_data_string_bowl']}\n"}

# COMMAND ----------

response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "system", "content": "You are a professional cricket commentator. Also make the following assumptions:\n - write in past tense\n - ONLY USE THE PROVIDED LIST AND NO EXTERNAL INFORMATION OTHERWISE I'LL BE ANGRY\n - first innings team refers to the team that first batted"},
        {"role": "user", "content": f"Summarise the following game as a commentator without the toss: {row_string}\n Create an additional paragraph from the following string about the batters of the above described game from the following: {row['all_data_string_bat']}\n Create an additional paragraph from the following about the bowlers of the above described game from the following: {row['all_data_string_bowl']}\n"}
    ]
)

text_result = response['choices'][0]['message']['content'],
text_result

# COMMAND ----------

response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "user", "content": f"You are a professional cricket commentator. Commentate only on the provided information: {row_string}\n With this information in mind, create a another paragraph from the following information: {row['all_data_string_bat']}\n and likewise from the following information: {row['all_data_string_bowl']}"}
    ]
)

text_result = response['choices'][0]['message']['content'],
text_result

# COMMAND ----------

print(text_result)

# COMMAND ----------

result_strings_all = result_strings + result_strings_2
len(result_strings_all)

# COMMAND ----------

master_df_filtered['all_data_string'] = result_strings_all
master_df_filtered.head(2)

# COMMAND ----------

master_df_filtered_string = master_df_filtered.to_spark()
display(master_df_filtered_string)

# COMMAND ----------

# MAGIC %md
# MAGIC #  Master Bowl DF All Text Column
# MAGIC Text preprocessing for faster and more effective LLM queries

# COMMAND ----------

master_bowl = spark.read.csv('dbfs:/FileStore/super-cricket/master_bowl_filtered_series.csv', header = True, inferSchema = True).pandas_api()
master_bowl = master_bowl.rename(columns={"O":"overs", "M":"maiden_overs", "R":"runs", "W":"wickets", "ECON":"economy", "WD":"wide", "NB":"no_ball"})
master_bowl.head(2)

# COMMAND ----------

# Get unique values for multiple columns
unique_values = master_bowl[['country', 'game', 'series']].drop_duplicates()

# Initialize an empty list to store the resulting strings
result_strings = []

# Iterate through the rows of the DataFrame
for index, row in tqdm(unique_values.iterrows(), total=len(unique_values), desc="Processing Rows"):
    # Filtered Master
    df_filter = master_bowl[(master_bowl['game']==row['game']) & \
                            (master_bowl['series']==row['series']) & \
                            (master_bowl['country']==row['country'])].drop(columns=['country', 'game', 'series'])

    df_string = ''
    for index_filter, row_filter in df_filter.iterrows():
        # Create a list of "column_name: value" pairs with semicolon separator, excluding specified columns
        column_value_pairs = [f'{column}:{value}' for column, value in row_filter.items() if column not in columns_to_exclude]

        # Join the pairs into a single string with semicolon-separated values
        row_string = '; '.join(column_value_pairs)
        df_string = df_string + '\n' + row_string
    
    df_string = f"game: {row['game']}; series: {row['series']}; country: {row['country']}" + '\n' + df_string

    response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a professional cricket commentator you must assume all questions are cricket related."},
                    {"role": "user", "content": f"Take the following information and create a concise very short commentary that must be less than 50 words but including all the information from the following: {df_string}"}
                ]
            )
    
    text_result = response['choices'][0]['message']['content']

    # Append the row string to the result_strings list
    result_strings.append(text_result)

# COMMAND ----------

result_strings

# COMMAND ----------

unique_values['all_data_string'] = result_strings
master_bowl_string = unique_values.copy()
master_bowl_string.head(2)

# COMMAND ----------

master_bowl_string = master_bowl_string.to_spark()
display(master_bowl_string)

# COMMAND ----------

# MAGIC %md
# MAGIC #  Master Bat DF All Text Column
# MAGIC Text preprocessing for faster and more effective LLM queries

# COMMAND ----------

master_bat = spark.read.csv('dbfs:/FileStore/super-cricket/master_bat_filtered_series.csv', header = True, inferSchema = True).pandas_api()
master_bat = master_bat.rename(columns={"O":"overs","R":"runs", "SR":"strike_rate"}).drop(columns=['B', 'M'])
master_bat.head(2)

# COMMAND ----------

# Get unique values for multiple columns
unique_values = master_bat[['country', 'game', 'series']].drop_duplicates()

# Initialize an empty list to store the resulting strings
result_strings = []

# Iterate through the rows of the DataFrame
for index, row in tqdm(unique_values.iterrows(), total=len(unique_values), desc="Processing Rows"):
    # Filtered Master
    df_filter = master_bat[(master_bat['game']==row['game']) & \
                            (master_bat['series']==row['series']) & \
                            (master_bat['country']==row['country'])].drop(columns=['country', 'game', 'series'])

    df_string = ''
    for index_filter, row_filter in df_filter.iterrows():
        # Create a list of "column_name: value" pairs with semicolon separator, excluding specified columns
        column_value_pairs = [f'{column}:{value}' for column, value in row_filter.items() if column not in columns_to_exclude]

        # Join the pairs into a single string with semicolon-separated values
        row_string = '; '.join(column_value_pairs)
        df_string = df_string + '\n' + row_string
    
    df_string = f"game: {row['game']}; series: {row['series']}; country: {row['country']}" + '\n' + df_string

    response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a professional cricket commentator you must assume all questions are cricket related."},
                    {"role": "user", "content": f"Take the following information and create a concise very short commentary that must be less than 50 words but including all the information from the following: {df_string}"}
                ]
            )
    
    text_result = response['choices'][0]['message']['content']

    # Append the row string to the result_strings list
    result_strings.append(text_result)

# COMMAND ----------

unique_values['all_data_string'] = result_strings
master_bat_string = unique_values.copy()
master_bat_string.head(2)

# COMMAND ----------

master_bat_string = master_bat_string.to_spark()
display(master_bat_string)

# COMMAND ----------

loader_master = PySparkDataFrameLoader(spark, master_df_filtered_string, page_content_column="all_data_string")
documents_master = loader_master.load()

loader_bowl = PySparkDataFrameLoader(spark, master_bowl_string, page_content_column="all_data_string")
documents_bowl = loader_bowl.load()

loader_bat = PySparkDataFrameLoader(spark, master_bat_string, page_content_column="all_data_string")
documents_bat = loader_bat.load()

# COMMAND ----------

text_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=0)

master_texts = text_splitter.split_documents(documents_master)
bowl_texts = text_splitter.split_documents(documents_bowl)
bat_texts = text_splitter.split_documents(documents_bat)

# COMMAND ----------

documents_all = documents_master + documents_bowl + documents_bat
documents_all

# COMMAND ----------

os.environ['OPENAI_API_KEY'] = "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"

# create embeddings and store in vectordb
vectordb = Pinecone.from_documents(
  documents_all,
  embedding = OpenAIEmbeddings(chunk_size = 1),
  index_name = 'super-predictor'
)

# COMMAND ----------

file_path_master = dbutils.fs.ls('dbfs:/FileStore/super-cricket/master_df_filtered.csv')[-1][0]
file_path_master = replace_dbfs_prefix(file_path_master)

file_path_bowl = dbutils.fs.ls('dbfs:/FileStore/super-cricket/master_bowl.csv')[-1][0]
file_path_bowl = replace_dbfs_prefix(file_path_bowl)

file_path_bat = dbutils.fs.ls('dbfs:/FileStore/super-cricket/master_bat.csv')[-1][0]
file_path_bat = replace_dbfs_prefix(file_path_bat)

# COMMAND ----------

documents_master = CSVLoader(file_path=file_path_master).load()
documents_bowl = CSVLoader(file_path=file_path_bowl).load()
documents_bat = CSVLoader(file_path=file_path_bat).load()

# COMMAND ----------

#split document into chunks
text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=200)

documents_master = text_splitter.split_documents(documents_master)
documents_bowl = text_splitter.split_documents(documents_bowl)
documents_bat = text_splitter.split_documents(documents_bat)

documents_all = documents_master + documents_bowl + documents_bat

# COMMAND ----------

documents_all

# COMMAND ----------

os.environ['OPENAI_API_KEY'] = "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"

# create embeddings and store in vectordb
vectordb = Pinecone.from_documents(
  documents_all,
  embedding = OpenAIEmbeddings(chunk_size = 1),
  index_name = 'super-predictor'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scrap Code

# COMMAND ----------

loader_master = PySparkDataFrameLoader(spark, master_df_filtered, page_content_column="result")
documents_master = loader_master.load()

loader_bowl = PySparkDataFrameLoader(spark, master_bowl, page_content_column="BOWLING")
documents_bowl = loader_bowl.load()

loader_bat = PySparkDataFrameLoader(spark, master_bat, page_content_column="BATTING")
documents_bat = loader_bat.load()
 
# text_splitter = RecursiveCharacterTextSplitter(chunk_size=3000, chunk_overlap=0)
# texts = text_splitter.split_documents(documents)
# print(f"Number of documents: {len(texts)}")

# COMMAND ----------

file_name = 'dbfs:/FileStore/super-cricket/master_bowl.csv'
file_name_list = dbutils.fs.ls(file_name)
part_name = '/' + [file_info.name for file_info in file_name_list if file_info.name.startswith('part-00000')][0]
    
file_name += part_name 
### file_name variables for each csv and then upsert each new csvloader version of the data...

# COMMAND ----------

# master_df_filtered = spark.read.csv('dbfs:/FileStore/super-cricket/master_df_filtered.csv', header = True, inferSchema = True)
master_bowl = spark.read.csv('dbfs:/FileStore/super-cricket/master_bowl_filtered_series.csv', header = True, inferSchema = True)
master_bat = spark.read.csv('dbfs:/FileStore/super-cricket/master_bat_filtered_series.csv', header = True, inferSchema = True)

# Convert the date column to a string with a desired format - not sure if necessary
# date_format_pattern = "yyyy-MM-dd"
# maters_df_filtered = master_df_filtered.withColumn("date", date_format("date", date_format_pattern))

# master_df_filtered = master_df_filtered.pandas_api()
# display(master_df_filtered.head(2))

# COMMAND ----------

display(master_df_filtered)

# COMMAND ----------

display(master_bowl)

# COMMAND ----------

display(master_bat)

# COMMAND ----------

# Specify the columns to exclude
columns_to_exclude = ['scorecard_url', 'ball_by_ball_commentary_url']

# Initialize an empty list to store the resulting strings
result_strings = []

# Iterate through the rows of the DataFrame
for index, row in tqdm(master_df_filtered.iterrows(), total=len(master_df_filtered), desc="Processing Rows"):
    if index==0:
        # Create a list of "column_name: value" pairs with semicolon separator, excluding specified columns
        column_value_pairs = [f'{column}:{value}' for column, value in row.items() if column not in columns_to_exclude]
        
        # Join the pairs into a single string with semicolon-separated values
        row_string = '; '.join(column_value_pairs)

        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a professional cricket commentator you must assume all questions are cricket related."},
                {"role": "user", "content": f"Take the following information and create a concise very short commentary that must be less than 50 words but including all the information from the following: {row_string}"}
            ]
        )
        
        text_result = response['choices'][0]['message']['content']

        # Append the row string to the result_strings list
        result_strings.append(text_result)


result_strings