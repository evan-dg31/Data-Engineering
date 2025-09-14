# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

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

# ADD WIDGETS
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")



# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

pits_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                   StructField('driverId', IntegerType(), True),
                                   StructField('stop', StringType(), True),
                                   StructField('lap', IntegerType(), False),
                                   StructField('time', StringType(), True),
                                   StructField('duration', StringType(), False),
                                   StructField('milliseconds', IntegerType(), True)
                                ])

# COMMAND ----------

pit_stops_df = spark.read.schema(pits_stops_schema).option("multiLine", True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# pit_stops_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json', schema=pits_stops_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion date

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed('driverId', 'driver_id')\
                                    .withColumnRenamed('raceId', 'race_id') \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

final_df = add_ingestion_date(final_df)

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed

# COMMAND ----------


# final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/pit_stops")
# final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.pit_stops")


# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")