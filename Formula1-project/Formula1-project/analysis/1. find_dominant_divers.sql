-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Pilotos Dominantes
-- MAGIC Os resultados das consultas abaixo podem ser salvos em tabelas ou views para que uma equipe de analistas possam usá-los.

-- COMMAND ----------

-- DBTITLE 1,Classificação dos pilotos de acordo com a média de pontos
-- só será incluído os pilotos que participaram de pelo menos 50 corridas (HAVING COUNT(1) >= 50) 
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- DBTITLE 1,Classificação dos pilotos entre 2011 e 2020 
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

-- DBTITLE 1,Classificação dos pilotos entre 2001 e 2010 
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC