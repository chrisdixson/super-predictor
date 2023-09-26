-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Database setup
-- MAGIC File for setting up database tables and running optimisation
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS super_predictor_v2;
CREATE TABLE IF NOT EXISTS super_predictor_v2.master_bowl;
CREATE TABLE IF NOT EXISTS super_predictor_v2.master_bat;
CREATE TABLE IF NOT EXISTS super_predictor_v2.master_results;

-- COMMAND ----------

OPTIMIZE super_predictor_v2.master_bowl;
VACUUM super_predictor_v2.master_bowl;
OPTIMIZE super_predictor_v2.master_bat;
VACUUM super_predictor_v2.master_bat;
OPTIMIZE super_predictor_v2.master_results;
VACUUM super_predictor_v2.master_results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##DB Testing
-- MAGIC Testing to validate tables are correct and refreshes are functional

-- COMMAND ----------

select count(*) from super_predictor_v2.master_bowl;

-- COMMAND ----------

-- Check for duplicates, should have same count as above
select count(*)
from super_predictor_v2.master_bowl A
join super_predictor_v2.master_bowl B
  on A.BOWLING = B.BOWLING
  and A.game = B.game
  and A.series = B.series

-- COMMAND ----------

select count(*) from super_predictor_v2.master_bat

-- COMMAND ----------

-- Check for duplicates, should have same count as above
select count(*)
from super_predictor_v2.master_bat A
join super_predictor_v2.master_bat B
  on A.BATTING = B.BATTING
  and A.game = B.game
  and A.series = B.series

-- COMMAND ----------

select count(*) from super_predictor_v2.master_results

-- COMMAND ----------

-- Check for duplicates, should have same count as above
select count(*)
from super_predictor_v2.master_results A
join super_predictor_v2.master_results B
  on A.date = B.date
  and A.game_name = B.game_name
