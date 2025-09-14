# Databricks notebook source
# MAGIC %md ### Ingest circuits.csv file
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit


# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the CSV fil using th spark dataframe reader

# COMMAND ----------

# Create a schema for the DataFrame
circuits_schema = StructType(fields=[StructField('circuitId', IntegerType(), False), 
                                     StructField('circuitRef', StringType(), True), 
                                     StructField('name', StringType(), True), 
                                     StructField('location', StringType(), True), 
                                     StructField('country', StringType(), True),
                                     StructField('lat', DoubleType(), True),
                                     StructField('lng', DoubleType(), True),
                                     StructField('alt', IntegerType(), True),
                                     StructField('url', StringType(), True)])

# COMMAND ----------

# circuits_df = spark.read.csv("/mnt/formula1edgdl/raw/circuits.csv")
# circuits_df = spark.read.csv("/mnt/formula1edgdl/raw/circuits.csv", header=True)
circuits_df = spark.read \
                    .option("header", True) \
                    .schema(circuits_schema) \
                    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Select the required columns

# COMMAND ----------

# Select the required columns from the dataframe
circuits_selected_df = circuits_df.select(col("circuitId"), 
                                          col("circuitRef"), 
                                          col("name"), 
                                          col("location"), 
                                          col("country"), 
                                          col("lat"), 
                                          col("lng"), 
                                          col("alt") )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename Columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                          .withColumnRenamed("circuitRef", "circuit_ref") \
                                          .withColumnRenamed("lat", "latitude") \
                                          .withColumnRenamed("lng", "longitude") \
                                          .withColumnRenamed("alt", "altitude") \
                                          .withColumn("data_source", lit(v_data_source)) \
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

# Add a new column 'ingestion_updated' with the current timestamp
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to Data Lake as parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed;

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# Save in external storage and create a manage table in f1_processed database
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")