# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Available Data
# MAGIC
# MAGIC These file paths will return the following files:
# MAGIC
# MAGIC root - dbfs:/FileStore/super-predict/
# MAGIC | File Name | Description |
# MAGIC | --------- | ----------- |
# MAGIC | master_df.csv | master_df with all odi matches from 2018 |
# MAGIC | master_df_filtered.csv | df filtered from 2022 and including only matches where both teams are world cup teams |
# MAGIC | master_bowl.csv        | all bowling scorecards from 2018 concatenated into a single csv file |
# MAGIC | master_bat.csv         | all batting scorecards from 2018 concatenated into a single csv file |
# MAGIC
# MAGIC To do:
# MAGIC
# MAGIC - Save all commentary data txts to a folder for the master_df_filtered games
# MAGIC - Upload commentary data txts + bowl_master_filtered + bat_master_filtered to PineCone DB
# MAGIC - Save relevant news articles txts to a folder for the master_df_filtered games (once other data is loaded )
# MAGIC - Connect to github and pull into personal dbricks env for easy file sharing

# COMMAND ----------

master_df = spark.read.csv('dbfs:/FileStore/super-cricket/master_df.csv', header = True, inferSchema = True)
master_df.show()

# COMMAND ----------

master_df_filtered = spark.read.csv('dbfs:/FileStore/super-cricket/master_df_filtered.csv', header = True, inferSchema = True)
master_df_filtered.show()

# COMMAND ----------

master_bowl = spark.read.csv('dbfs:/FileStore/super-cricket/master_bowl.csv', header = True, inferSchema = True)
master_bowl.show()

# COMMAND ----------

master_bat = spark.read.csv('dbfs:/FileStore/super-cricket/master_bat.csv', header = True, inferSchema = True)
master_bat.show()