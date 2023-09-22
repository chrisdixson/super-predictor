# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Get manual download links

# COMMAND ----------

root = 'dbfs:/FileStore/super-cricket/'

def url_download(root, file_name):
    dbrick_id       = '?o=104094482268475'
    dbrick_instance = 'https://enb-ey.cloud.databricks.com/'
    url_path        = root.replace('dbfs:/FileStore','files')
    file_name_list = dbutils.fs.ls(f'{root}/{file_name}')
    part_name = '/' + [file_info.name for file_info in file_name_list if file_info.name.startswith('part-00000')][0]
    link = dbrick_instance + url_path + file_name + part_name + dbrick_id
    print(link)

url_download(root,'master_bat_filtered.csv')
url_download(root,'master_bowl_filtered.csv')
url_download(root, 'master_df_filtered.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Check last edit time

# COMMAND ----------

import pytz

def check_path_mod_date(path):
    timestamp_unix = dbutils.fs.ls(path)[0].modificationTime / 1000
    datetime_utc   = datetime.utcfromtimestamp(timestamp_unix)
    aest_timezone  = pytz.timezone("Australia/Sydney")
    datetime_aest  = datetime_utc.astimezone(aest_timezone)
    return datetime_aest

print(check_path_mod_date(master_df_path))