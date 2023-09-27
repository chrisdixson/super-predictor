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

display(spark.read.parquet('dbfs:/FileStore/super-cricket/master_all_data_v2.parquet', header = True, inferSchema = True))
