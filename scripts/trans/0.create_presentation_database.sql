-- Databricks notebook source
USE CATALOG hive_metastore

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1edgdl/presentation"


-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_presentation

-- COMMAND ----------

