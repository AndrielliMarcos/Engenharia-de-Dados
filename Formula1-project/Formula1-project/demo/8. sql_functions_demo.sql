-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

SELECT * FROM drivers

-- COMMAND ----------

-- concatenar 2 campos e colovar um '-' entre eles
SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM drivers

-- COMMAND ----------

-- separar um campo em 2
SELECT *, SPLIT(name, ' ')
FROM drivers

-- COMMAND ----------

-- separar um campo em 2
SELECT *, SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[1] surname
FROM drivers

-- COMMAND ----------

-- inserir uma coluna de data e hora atuais
SELECT *, current_timestamp()
FROM drivers

-- COMMAND ----------

-- alterar o formato de data
SELECT *, date_format(dob,'dd-MM-yyyy' )
FROM drivers

-- COMMAND ----------

-- adicionar 1 dia na data
SELECT *, date_add(dob, 1)
FROM drivers

-- COMMAND ----------

-- data máxima de nacimento de um piloto
SELECT MAX(dob)
FROM drivers;

-- COMMAND ----------

-- saber qual é o piloto com a data de nascimento máxima
SELECT * FROM drivers
WHERE dob = '2000-05-11'

-- COMMAND ----------

-- quantidade de pilotos britânicos
SELECT count(*)
FROM drivers
WHERE nationality = 'British'

-- COMMAND ----------

-- lista de pilotos em relação a cada nacionalidade e ordenar por nacionalidade
-- group by

SELECT nationality,count(*)
FROM drivers
GROUP BY nationality
ORDER BY nationality;

-- COMMAND ----------

-- obter países que tem mais de 100 motoristas
-- HAVING
SELECT nationality, count(*)
FROM drivers
GROUP BY nationality
HAVING count(*) > 100 
ORDER BY nationality;

-- COMMAND ----------

-- ordenar pela data de nascimento, de modo a classificar (rank) decrescente os motoristas no topo
-- particionar por nacionalidade
-- RANK()
SELECT nationality, name, dob, RANK() OVER (PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality, age_rank