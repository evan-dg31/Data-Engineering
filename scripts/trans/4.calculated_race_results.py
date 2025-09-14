# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS hive_metastore.f1_presentation.calculated_race_results
            (
                race_year INT,
                team STRING,
                driver_id INT,
                driver_name STRING,
                race_id INT,
                position INT,
                points INT,
                calculated_points INT,
                created_date TIMESTAMP,
                updated_date TIMESTAMP
            )
            USING DELTA
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE hive_metastore.f1_presentation

# COMMAND ----------

# %sql
# MERGE INTO f1_presentation.calculated_race_results tgt
# USING _sqldf src
# ON tgt.race_year = src.race_year AND tgt

# COMMAND ----------

spark.sql("USE hive_metastore.f1_processed")
spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW race_results_updated
            AS
            SELECT  races.race_year, 
                    constructors.name AS team_name, 
                    drivers.driver_id,
                    drivers.name AS driver_name, 
                    races.race_id,
                    results.position, 
                    results.points,
                    11 - results.position AS calculated_points

            FROM f1_processed.results
                INNER JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
                INNER JOIN f1_processed.constructors ON (constructors.constructor_id = results.constructor_id)
                INNER JOIN f1_processed.races ON (races.race_id = results.race_id)
                
            WHERE results.position <= 10
                AND results.file_date = '{v_file_date}'
            """)


# COMMAND ----------

spark.sql(f"""
                MERGE INTO hive_metastore.f1_presentation.calculated_race_results AS tgt
                USING race_results_updated AS upd
                ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
                WHEN MATCHED THEN
                    UPDATE SET tgt.position = upd.position,
                            tgt.points = upd.points,
                            tgt.calculated_points = upd.calculated_points,
                            tgt.updated_date = current_timestamp
                WHEN NOT MATCHED THEN 
                    INSERT (race_year, team, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
                    VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)
             """)
