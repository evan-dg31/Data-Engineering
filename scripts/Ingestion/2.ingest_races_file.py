# Databricks notebook source
# MAGIC %md ### Ingest races.csv file
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from  pyspark.sql.types import IntegerType, DoubleType, StructType, StructField,StringType, DateType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Add Widgets
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                  StructField('year', IntegerType(), True),
                                  StructField('round', IntegerType(), True),
                                  StructField('circuitId', IntegerType(), True),
                                  StructField('name', StringType(), True),
                                  StructField('date', DateType(), True),
                                  StructField('time', StringType(), True),
                                  StructField('url', StringType(), True)
                                ])

# COMMAND ----------

races_df = spark.read \
        .option("header", True) \
        .schema(races_schema) \
        .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn("data_source", lit(v_data_source)) \
                                  .withColumn("file_date", lit(v_file_date))

races_with_timestamp_df = add_ingestion_date(races_with_timestamp_df)

                                

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Select the required columns and rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), 
                                                   col('year').alias('race_year'), 
                                                   col('round'), 
                                                   col('circuitId').alias('circuit_id'), 
                                                   col('name'), 
                                                   col('date'), 
                                                   col('time'), 
                                                   col('ingestion_date'),
                                                   col('race_timestamp'),
                                                   col('data_source'),
                                                   col('file_date'),
                                                   ) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed folder in Delta Table format

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed;

# COMMAND ----------

# races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1edgdl/processed/races')
races_selected_df.write.mode('overwrite').partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")