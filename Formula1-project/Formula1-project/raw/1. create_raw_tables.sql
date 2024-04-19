-- Databricks notebook source
-- DBTITLE 1,Criação do banco de dados
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- DBTITLE 1,Tabela circuits
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
OPTIONS (path "/mnt/formula1-project/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits; 

-- COMMAND ----------

-- DBTITLE 1,Tabela races
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
OPTIONS (path "/mnt/formula1-project/raw/races.csv", header true)

-- COMMAND ----------

-- DBTITLE 1,Tabela constructors
-- arquivo JSON com estrutura simples (1 linha)
DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula1-project/raw/constructors.json")


-- COMMAND ----------

-- DBTITLE 1,Tabela drivers
-- arquivo JSON com estrutura complexa (1 linha, coluna name é aninhada)
DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING json
OPTIONS (path "/mnt/formula1-project/raw/drivers.json")

-- COMMAND ----------

-- DBTITLE 1,Tabela results
-- arquivo JSON com estrutura simples (1 linha)
DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
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
fastestLamp INT,
rank INT,
fastestLampTime STRING,
fastestLampSpeed FLOAT,
statusId STRING
)
USING json
OPTIONS (path "/mnt/formula1-project/raw/results.json")

-- COMMAND ----------

-- DBTITLE 1,Tabela pit_stops
-- arquivo JSON com estrutura simples com multilinhas
DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId INT,
driverId INT,
stop STRING,
lap STRING,
time STRING,
duration STRING,
milliseconds INT
)
USING json
OPTIONS (path "/mnt/formula1-project/raw/pit_stops.json", multiline true)


-- COMMAND ----------

-- DBTITLE 1,Tabela lap_times
-- pasta com vários arquivos em formato csv
DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
millisegundos STRING
)
USING csv
OPTIONS (path "/mnt/formula1-project/raw/lap_times")

-- COMMAND ----------

-- DBTITLE 1,Tabela qualifying
-- pasta com vários arquivos em formato JSON e os arquivos têm várias linhas
DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING json
OPTIONS (path "/mnt/formula1-project/raw/qualifying", multiline true)