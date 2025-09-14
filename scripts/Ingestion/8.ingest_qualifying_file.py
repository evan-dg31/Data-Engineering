# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
from pyspark.sql.functions import col, current_timestamp, concat, lit
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# ADDD WIDGETS
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")



# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField('qualifyId', IntegerType(), False),
                                   StructField('raceId', IntegerType(), True),
                                   StructField('driverId', IntegerType(), True),  
                                   StructField('constructorId', StringType(), True),
                                   StructField('number', IntegerType(), True),
                                   StructField('position', IntegerType(), True),
                                   StructField('q1', StringType(), True),
                                   StructField('q2', StringType(), True),
                                   StructField('q3', StringType(), True)
                                ])

# COMMAND ----------

qualifying_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/qualifying', multiLine=True, schema=qualifying_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion date

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
                                 .withColumnRenamed('driverId', 'driver_id') \
                                 .withColumnRenamed('raceId', 'race_id') \
                                 .withColumnRenamed('constructorId', 'constructor_id') \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))
final_df = add_ingestion_date(final_df) 

display(final_df )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write output to processed container in parquet format
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

display(_sqldf)