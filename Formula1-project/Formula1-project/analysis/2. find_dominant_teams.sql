-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Equipes dominantes

-- COMMAND ----------

-- DBTITLE 1,Classificação das equipes de acordo com a média de pontos
-- serão incluídas pelo menos 100 corridas, já que cada equipe tem 2 pilotos
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- DBTITLE 1,Classificação das equipes entre 2011 e 2020 
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

-- DBTITLE 1,Classificação das equipes entre 2001 e 2010 
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC