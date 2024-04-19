-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Managed Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # salvar o dataframe no database demo (hive meta store)
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### External Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # salvar o dataframe no database demo no Data Lake
-- MAGIC # neste caso a path deve ser especificada
-- MAGIC race_results_df.write.format('parquet').option('path', f'{presentation_folder_path}/race_results_ext_py').saveAsTable('demo.race_results_ext_py')

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

-- criar uma tabela externa
CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING PARQUET
LOCATION "dbfs:/mnt/formula1-project/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- inserir os dados da tabela 'race_results_ext_py' na tabela 'race_results_ext_sql'
INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT count(1) FROM demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC A view é uma representação visual dos dados de uma tabela.

-- COMMAND ----------

-- criar uma view temporária
CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

-- criar uma view temporária global
CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- criar uma view permanente
-- essa view será armazenada no Hive Meta Store
CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES;