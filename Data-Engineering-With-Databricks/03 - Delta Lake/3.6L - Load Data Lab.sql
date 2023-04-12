-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Carregar Dados - Lab
-- MAGIC Neste laboratório iremos carregar dados para tabelas Deltas novas e existentes.
-- MAGIC 
-- MAGIC ###Objetivo
-- MAGIC - Criar uma tabela Delta vazia com um schema fornecido
-- MAGIC - Inserir registros de uma tabela existente para uma tabela Delta
-- MAGIC - Usar a indtrução CTAS para criar uma tabela Delta de arquivos

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.6L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Visão Geral dos Dados
-- MAGIC Iremos trabalhar com uma amostra de dados Kafka brutos escritos como arquivos JSON.
-- MAGIC 
-- MAGIC Cada arquivo contém todos os registros consumidos durante um intervalo de 5 segundos, armazenados com o schema Kafka completo como um arquivo JSON de vários registros.
-- MAGIC 
-- MAGIC O schema para esta tabela:
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
-- MAGIC ###Definir schema para uma tabela Delta vazia
-- MAGIC Criar uma tabela gerenciada vazia chamada **`events_raw`** usado o mesmo schema.

-- COMMAND ----------

-- resposta
CREATE TABLE IF NOT EXISTS events_raw
(key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula a seguir para confirmar se a tabela foi criada corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw"), "Table named `events_raw` does not exist"
-- MAGIC assert spark.table("events_raw").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_raw").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC assert spark.table("events_raw").count() == 0, "The table should have 0 records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inserir eventos brutos na tabela Delta
-- MAGIC Assim que os dados extraídos e a tabela Delta estiverem prontos, insira os registros da tabela **`events_json`** na nova tabela **`events_raw`**.

-- COMMAND ----------

-- resposta
INSERT INTO events_raw
SELECT * FROM events_json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Revise manualmente o conteúdo da tabela para garantir que ps dados foram escritos como esperado.

-- COMMAND ----------

-- resposta
SELECT * FROM events_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula a seguir para confirmar que os dados tenham sido carregados corretamente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw").count() == 2252, "The table should have 2252 records"
-- MAGIC assert set(row['timestamp'] for row in spark.table("events_raw").select("timestamp").limit(5).collect()) == {1593880885085, 1593880892303, 1593880889174, 1593880886106, 1593880889725}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar uma tabela Delta a partir do resultado de uma consulta
-- MAGIC Além dos novos dados de eventos, vamos também carregar uma pequena tabela de pesquisa que fornece detalhes do produto que usaremos posteriormente no curso. 
-- MAGIC 
-- MAGIC Use uma istrução CTAS para criar uma tabela Delta gerenciada chamada **`item_lookup`** que extrai dados do diretório parquet fornecido abaixo.

-- COMMAND ----------

-- resposta
CREATE OR REPLACE TABLE item_lookup
AS SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/item-lookup`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("item_lookup").count() == 12, "The table should have 12 records"
-- MAGIC assert set(row['item_id'] for row in spark.table("item_lookup").select("item_id").orderBy('item_id').limit(5).collect()) == {'M_PREM_F', 'M_PREM_K', 'M_PREM_Q', 'M_PREM_T', 'M_STAN_F'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

SELECT * FROM item_lookup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
