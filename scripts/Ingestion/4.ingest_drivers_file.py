# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
from pyspark.sql.functions import col, current_timestamp, concat, lit


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

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

name_schema =  StructType(fields=[StructField('forename', StringType(), True),
                                   StructField('surname', StringType(), True)
])


drivers_schema = StructType(fields=[StructField('driverId', IntegerType(), True),
                                    StructField('driverRef', StringType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('code', StringType(), True),
                                    StructField('name', name_schema, True),
                                    StructField('dob', DateType(), True),
                                    StructField('nationality', StringType(), True),
                                    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId rename to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname

# COMMAND ----------

# drivers_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
#                                     .withColumnRenamed('driverRef', 'driver_ref') \
#                                     .withColumn('ingestion_date', current_timestamp()) \
#                                     .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))

drivers_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('driverRef', 'driver_ref') \
                                    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

drivers_with_columns_df = add_ingestion_date(drivers_with_columns_df)

display(drivers_with_columns_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to processed container in parquet format
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed

# COMMAND ----------

# drivers_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/drivers')
# drivers_final_df.write.mode('overwrite').format("parquet").saveAsTable('f1_processed.drivers')

drivers_final_df.write.mode('overwrite').format("delta").saveAsTable('f1_processed.drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")