# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

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

constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

# constructors_dropped_df = constructors_df.drop('url')
# constructors_dropped_df = constructors_df.drop(col('url')) 
constructors_dropped_df = constructors_df.drop(constructors_df['url'])
#display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestions date

# COMMAND ----------


# constructors_final_df = constructors_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
#                                                .withColumnRenamed('constructorRef', 'constructor_ref') \
#                                                .withColumn('ingestion_date', current_timestamp())

constructors_final_df = constructors_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
                                               .withColumnRenamed('constructorRef', 'constructor_ref') \
                                               .withColumn("data_source", lit(v_data_source)) \
                                               .withColumn("file_date", lit(v_file_date))

constructors_final_df = add_ingestion_date(constructors_final_df)

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_processed;

# COMMAND ----------

# constructors_final_df.write.mode('overwrite').parquet(f"{processed_folder_path}/constructors")
constructors_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")