-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Laboratório de Extração de Dados
-- MAGIC Neste laboratório, vamos extrair dados brutos de arquivos JSON.
-- MAGIC 
-- MAGIC ### Objetivos
-- MAGIC - Registrar uma tabela externa para extrair dados de arquivos JSON.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.3L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Visão geral dos dados
-- MAGIC Nós vamos trabalhar com uma amostra de dados brutos Kafka escritos como arquivos JSON.
-- MAGIC 
-- MAGIC Cada arquivo contem todos os registros consumidos durante um intervalo de 5 segundos, armazenado com o schema Kafka completo como um arquivo JSON de vários registros.
-- MAGIC 
-- MAGIC O schema para a tabela:
-- MAGIC | field  | type | description |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key    | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | timestamp | LONG    | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Extrair eventos brutos de arquivos JSON
-- MAGIC Para carregar este dado para dentro do Delta apropriadamente, nós primeiro precisamos extrair o dado JSON usando o schema correto.
-- MAGIC 
-- MAGIC Crie uma tabela externa de arquivo JSON lacalizada na path a seguir. O nome desta tabela é **`events_json`** e declare o schema a seguir.

-- COMMAND ----------

-- Resposta
-- Para ler um arquivo JSON: SELECT * FROM JSON.(path/file)
-- Para criar uma tabela: colunas + tipo, formato, location
CREATE TABLE IF NOT EXISTS events_json
(key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY)
USING JSON
LOCATION "${DA.paths.kafka_events}"

-- COMMAND ----------

SELECT * FROM events_json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Observação:** usaremos Python para executar verificações. A célula a seguir irá retornar um erro com uma mensagem do que precisa mudar se você não tiver seguido as instruções

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC 
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
