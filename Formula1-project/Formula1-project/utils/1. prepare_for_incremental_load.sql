-- Databricks notebook source
-- DBTITLE 1,Deletar banco de dados f1_processed
DROP DATABASE IF EXISTS f1_processed CASCADE;


-- COMMAND ----------

-- DBTITLE 1,Criar banco de dados f1_processed
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/formula1-project/processed';

-- COMMAND ----------

-- DBTITLE 1,Deletar banco de dados f1_presentation
DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

-- DBTITLE 1,Criar banco de f1_presentation
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/formula1-project/presentation';