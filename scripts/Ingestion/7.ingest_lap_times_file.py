# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
from pyspark.sql.functions import col, current_timestamp, concat, lit
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# ADD WIDGETS
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")



# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                   StructField('driverId', IntegerType(), True),  
                                   StructField('stop', StringType(), True),
                                   StructField('lap', IntegerType(), True),
                                   StructField('time', StringType(), True),
                                   StructField('duration', StringType(), True),
                                   StructField('milliseconds', IntegerType(), True)
                                ])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion date

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed('driverId', 'driver_id')\
                                 .withColumnRenamed('raceId', 'race_id') \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

lap_times_final_df = add_ingestion_date(lap_times_final_df)

                                     
display(lap_times_final_df )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed

# COMMAND ----------

# lap_times_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
# lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id AND tgt.stop = src.stop"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")