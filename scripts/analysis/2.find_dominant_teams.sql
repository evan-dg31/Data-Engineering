-- Databricks notebook source
SELECT current_catalog()

-- COMMAND ----------

SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

-- Dominant Teams from 2011 to 2020
SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

-- Dominant Teams from 2001 to 2011
SELECT team_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       ROUND(AVG(calculated_points),2) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING total_races >= 100
ORDER BY avg_points DESC;