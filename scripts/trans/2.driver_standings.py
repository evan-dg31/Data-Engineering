# Databricks notebook source
from pyspark.sql.functions import desc, rank, asc, sum, when, count, col
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the race years for which the data is to be re-processed

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentations_folder_path}/race_results") \
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

# Read the parquet file for the new race year is the year in the list as a filter

race_results_df = spark.read.format('delta').load(f"{presentations_folder_path}/race_results") \
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------


driver_standings_df = race_results_df.groupBy("race_year", "driver_id", "driver_name", "driver_nationality") \
        .agg(sum('points').alias('total_points'), count(when(col("position")==1, True)).alias('wins'))

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_presentation

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f"{presentations_folder_path}/driver_standings")
# final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.driver_standings")

# Write to a table
# overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentations_folder_path, merge_condition,'race_year')