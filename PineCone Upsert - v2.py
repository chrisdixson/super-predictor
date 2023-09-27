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
from langchain.docstore.document import Document
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

# # check if 'openai' index already exists (only create index if not)
# if 'super-predictor' not in pinecone.list_indexes():
#     pinecone.create_index('super-predictor', dimension=1536)
# # connect to index
# index = pinecone.Index('super-predictor')

# COMMAND ----------

#### DO ANOTHER VERSION THAT MAKES USE OF THE ORIGINAL NATURAL LANGUAGE DATA

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

file_path_master = dbutils.fs.ls('dbfs:/FileStore/super-cricket/master_df_filtered.csv')[-1][0]
file_path_master = replace_dbfs_prefix(file_path_master)

file_path_bowl = dbutils.fs.ls('dbfs:/FileStore/super-cricket/master_bowl.csv')[-1][0]
file_path_bowl = replace_dbfs_prefix(file_path_bowl)

file_path_bat = dbutils.fs.ls('dbfs:/FileStore/super-cricket/master_bat.csv')[-1][0]
file_path_bat = replace_dbfs_prefix(file_path_bat)

files = {"master_data":file_path_master, "bowler_data":file_path_bowl, "batter_data":file_path_bat}

# COMMAND ----------

# CSV loader metadata prep
metadata_dict = {}

for csv_file in files:
    metadata_dict[csv_file] = {"title": csv_file, "doc type": "csv"}

# COMMAND ----------

metadata_dict

# COMMAND ----------

documents = []

for csv_file in files:

    loader = CSVLoader(file_path = files[csv_file])

    # CSVLoader loads each row as a separate document by default

    # load the pdf file as a list of documents/rows
    doc = loader.load()

    # attach specified document metadata to each row (rows from same csv will have same metadata)
    for row in doc:
        documents.append(Document(page_content = row.page_content, metadata = metadata_dict[csv_file]))

# COMMAND ----------

# initialise embedding model
embed = OpenAIEmbeddings(
    model = 'text-embedding-ada-002',
    openai_api_key = "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"
)

# COMMAND ----------

# Setting index name
index_name = 'super-predictor-v2'

# COMMAND ----------

# get a list of all Pinecone indexes
indexes = pinecone.list_indexes()

# no need to re-upsert documents given existing index
upsert_flag = False

# check if the target index exists
if index_name not in indexes:
    # re-create a new index if it does not exist, otherwise use existing index
    pinecone.create_index(name = index_name, dimension = 1536, metric = "cosine")

    upsert_flag = True

# delete the existing index to avoid duplication
#pinecone.delete_index(index_name)

# COMMAND ----------

# connect to index
index = pinecone.Index(index_name)

# COMMAND ----------

tokenizer = tiktoken.encoding_for_model('text-embedding-ada-002')

# create length function
def tiktoken_len(text):
    tokens = tokenizer.encode(
        text,
        disallowed_special=()
    )
    return len(tokens)

# COMMAND ----------

# define text splitter
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200,
    length_function=tiktoken_len
)

# COMMAND ----------

# split documents into chunks
chunks = text_splitter.split_documents(documents)

# COMMAND ----------

MAX_INPUTS = 16
batch_size = MAX_INPUTS

# batch insert the chunks into the vector store to match max inputs allowed
for i in range(0, len(chunks), batch_size):
    chunk_batch = chunks[i:i + batch_size]
    Pinecone.from_documents(chunk_batch, embedding = embed, index_name = index_name)

# COMMAND ----------

# check if documents are upserted
vector_count = index.describe_index_stats()['total_vector_count']

if vector_count != 0:
    print(f'Documents upserted to vector store successfully: {vector_count} vectors.')

# COMMAND ----------

master_df = spark.read.csv('dbfs:/FileStore/super-cricket/master_df_filtered.csv', header = True, inferSchema = True)
display(master_df)

# COMMAND ----------

testingIndex = Pinecone.from_existing_index(index_name, embed)

# COMMAND ----------

query = "what matches were played on November 25th?"
testingIndex.similarity_search(
    query,  # our search query
    k=3  # return 3 most relevant docs
)

# COMMAND ----------


