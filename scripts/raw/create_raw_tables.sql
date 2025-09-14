-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create Database and External Tables for list of files
-- MAGIC

-- COMMAND ----------

-- Select a CATALOG to store the databases
USE CATALOG hive_metastore

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create f1_raw Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hive_metastore.f1_raw;


-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits tables

-- COMMAND ----------

-- Create a schema for the DataFrame
DROP TABLE IF EXISTS f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
                                            circuitId INT,
                                            circuitRef STRING,
                                            name STRING,
                                            location STRING,
                                            country STRING,
                                            lat DOUBLE,
                                            lng DOUBLE,
                                            alt INT,
                                            url STRING
)
USING csv
OPTIONS (path "/mnt/formula1edgdl/raw/circuits.csv", header = 'true')

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Races table

-- COMMAND ----------

-- Create a schema for the table
DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
                                        raceId INT,
                                        year INT,
                                        round INT,
                                        circuitId INT,
                                        name STRING,
                                        date DATE,
                                        time STRING,
                                        url STRING
)
USING csv
OPTIONS (path "/mnt/formula1edgdl/raw/races.csv", header = 'true')

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

-- Create a schema for the table
DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
                                                constructorId INT,
                                                constructorRef STRING,
                                                name STRING,
                                                nationality STRING,
                                                url STRING
)
USING json
OPTIONS (path "/mnt/formula1edgdl/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex Structure

-- COMMAND ----------

-- Create a schema for the table
DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
                                          driverId INT,
                                          driverRef STRING,
                                          number INT,
                                          code STRING,
                                          name STRUCT<forename:STRING, surname: STRING>,
                                          dob DATE,
                                          nationality STRING,
                                          url STRING
)
USING json
OPTIONS (path "/mnt/formula1edgdl/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

-- COMMAND ----------

-- Create a schema for the table
DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
                                          resultID INT,
                                          raceId INT,
                                          driverId INT,
                                          constructorId INT,
                                          number INT,
                                          grid INT,
                                          position INT,
                                          positionText STRING,
                                          positionOrder INT,
                                          points FLOAT,
                                          laps INT,
                                          time STRING,
                                          milliseconds INT,
                                          fastestLap INT,
                                          rank INT,
                                          fastestLapTime STRING,
                                          fastestLapSpeed FLOAT,
                                          statusId STRING
)
USING json
OPTIONS (path "/mnt/formula1edgdl/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Pit Stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

-- Create a schema for the table
DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
                                            driverId INT,
                                            duration STRING,
                                            lap INT,
                                            miliseconds INT,
                                            raceId INT,
                                            stop INT,
                                            time STRING
)
USING json
OPTIONS (path "/mnt/formula1edgdl/raw/pit_stops.json", multiLine = true)



-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Lap Times table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
                                            raceId INT,
                                            driverId INT,
                                            lap INT,
                                            positions INT,
                                            time STRING,
                                            milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1edgdl/raw/lap_times")


-- COMMAND ----------

SELECT COUNT(*) FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Qualifying External table
-- MAGIC * JSON file
-- MAGIC * Multiple JSON
-- MAGIC * Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
                                              constructorId INT,
                                              driverId INT,
                                              number INT,
                                              positions INT,
                                              q1 STRING,
                                              q2 STRING,
                                              q3 STRING,
                                              qualifyId INT,
                                              raceId INT
)
USING json
OPTIONS (path "/mnt/formula1edgdl/raw/qualifying", multiline = true)


-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED hive_metastore.f1_raw.qualifying

-- COMMAND ----------

