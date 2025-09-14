# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col

# Set the timezone to EST New York
spark.conf.set("spark.sql.session.timeZone", 'America/New_York')


def add_ingestion_date(input_df):
    ''' Add ingestion date to the dataframe '''

    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df







# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_names in input_df.schema.names:
        if column_names != partition_column:
            column_list.append(column_names)
    column_list.append(partition_column)
    output = input_df.select(column_list)
    print(column_list)
    return output

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    ''' Overwrite and insert into the table if it exists. If not create the table with a partition column '''

    output_df = re_arrange_partition_column(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode('overwrite').insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

    print(f"Incremental load of {db_name}.{table_name} completed. Partition by {partition_column}")


# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                          .distinct() \
                          .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

# Update the delta table if it exist if not create a new delta table.
def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    from delta.tables import DeltaTable
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            # race_id is the partition column and result_id is the unique key for the merge.
            merge_condition)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")