-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Drop All Tables

-- COMMAND ----------

DROP DATABASE IF EXISTS hive_metastore.f1_processed CASCADE;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.f1_processed
LOCATION '/mnt/formula1edgdl/processed';

-- COMMAND ----------

DROP DATABASE IF EXISTS hive_metastore.f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.f1_presentation
LOCATION '/mnt/formula1edgdl/presentation';