# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType, FloatType
from pyspark.sql.functions import col, current_timestamp, concat, lit
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_schema = StructType(fields=[StructField('resultId', IntegerType(), True),
                                   StructField('raceId', IntegerType(), True),
                                   StructField('driverId', IntegerType(), True),
                                   StructField('constructorId', IntegerType(), True),
                                   StructField('number', IntegerType(), False),
                                   StructField('grid', IntegerType(), True),
                                   StructField('position', IntegerType(), False),
                                   StructField('positionText', StringType(), True),
                                   StructField('positionOrder', IntegerType(), True),
                                   StructField('points', FloatType(), True),
                                   StructField('laps', IntegerType(), True),
                                   StructField('time', StringType(), False),
                                   StructField('milliseconds', IntegerType(), False),
                                   StructField('fastestLap', IntegerType(), False),
                                   StructField('rank', IntegerType(), False),
                                   StructField('fastestLapTime', StringType(), False),
                                   StructField('fastestLapSpeed', StringType(), False),
                                   StructField('statusId', IntegerType(), True),

                                ])

# COMMAND ----------

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. resultId rename to result_id
# MAGIC 2. raceId renamed to race_id
# MAGIC 3. driverId renamed to driver_id
# MAGIC 4. constructorId renamed to constructor_id
# MAGIC 5. postiontText renamed to position_text
# MAGIC 6. positionOrder renamed to position_order
# MAGIC 7. fastestLap renamed to fastest_lap
# MAGIC 8. fastestLapTime renamed to fastest_lap_time
# MAGIC 9. fastesLapSpeed renamed to fastest_lap_speed
# MAGIC 8. Add ingestion date

# COMMAND ----------


results_with_columns_df = results_df.withColumnRenamed('resultId', 'result_id') \
                                    .withColumnRenamed('raceId', 'race_id') \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('constructorId', 'constructor_id') \
                                    .withColumnRenamed('positionText', 'position_text') \
                                    .withColumnRenamed('positionOrder', 'position_order') \
                                    .withColumnRenamed('fastestLap', 'fastest_lap') \
                                    .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))
                                                

results_with_columns_df = add_ingestion_date(results_with_columns_df)

display(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns
# MAGIC 1. statusId

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to processed container in delta format and partion by race_id

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed

# COMMAND ----------

# results_final_df.write.mode('overwrite').partitionBy('race_id').parquet(f"{processed_folder_path}/results")
# results_final_df.write.mode('append').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1
# MAGIC - Loop and drop all the partition

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode('append').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")
# # #display(dbutils.fs.ls('/mnt/formula1edgdl/processed/results'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2 - Overwrite
# MAGIC - spark will find the partition and overwrite while it's tring to insert the data

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")