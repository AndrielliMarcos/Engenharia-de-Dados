-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inner Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Left Join

-- COMMAND ----------

-- obter todos os pilotos de 2018, independente de terem corrido em 2020 ou não
SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Right Outer Join

-- COMMAND ----------

-- obter todos os pilotos de 2020, independente de terem corrido em 2018 ou não
SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Full Join

-- COMMAND ----------

-- todos os pilotos que correram em 2018 e 2020, independente de ter nulos nos 2 anos
SELECT *
FROM v_driver_standings_2018 d_2018
FULL JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name)