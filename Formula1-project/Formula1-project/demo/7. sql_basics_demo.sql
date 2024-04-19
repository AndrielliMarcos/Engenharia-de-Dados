-- Databricks notebook source
-- listar os bancos de dados
SHOW DATABASES;

-- COMMAND ----------

-- verificar o banco de dados atual
SELECT current_database()

-- COMMAND ----------

-- alterar para o banco de dados desejado
USE f1_processed

-- COMMAND ----------

-- listar as tabelas do banco de dados atual
SHOW TABLES;

-- COMMAND ----------

-- selecionar todos os dados de uma tabela
-- o databricks limita automaticamente o número de linhas a 1000
SELECT * FROM drivers LIMIT 10

-- COMMAND ----------

-- descrever os nomes da colunas
DESC drivers

-- COMMAND ----------

-- clausula WHERE
SELECT * FROM drivers WHERE nationality = 'British'