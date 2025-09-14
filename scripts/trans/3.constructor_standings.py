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

race_results_df = spark.read.format('delta').load(f"{presentations_folder_path}/race_results") \
                            .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f"{presentations_folder_path}/race_results") \
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy("race_year","team") \
    .agg(sum('points').alias('total_points'), count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_presentation

# COMMAND ----------

# Save it to a table

# final_df.write.mode("overwrite").parquet(f"{presentations_folder_path}/constructor_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# overwrite_partition(final_df, "f1_presentation", "constructor_standings","race_year")

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentations_folder_path, merge_condition,'race_year')