-- Databricks notebook source
-- MAGIC %python
-- MAGIC print('hi')

-- COMMAND ----------

drop database if exists hive_metastore.f1_processed cascade;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.f1_processed
LOCATION "/mnt/sadevadls1/processed"

-- COMMAND ----------

drop database if exists hive_metastore.f1_presentation cascade;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.f1_prsentation
LOCATION "/mnt/sadevadls1/presentation";
