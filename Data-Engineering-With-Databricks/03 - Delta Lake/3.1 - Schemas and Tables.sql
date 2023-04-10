-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Schemas e Tabelas no Databricks
-- MAGIC 
-- MAGIC ###Objetivos
-- MAGIC - Usar Spark SQL DDL para definir schemas e tabelas
-- MAGIC - Descrver como a palavra chave **`LOCATION`** impacta o diretósio padrão de armazenamento
-- MAGIC 
-- MAGIC ###Recursos
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Schemas and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Schemas
-- MAGIC - Vamos criar uma schema com **`LOCATION`** e outro schema sem

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;
CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_custom_location LOCATION '${da.paths.working_dir}/${da.schema_name}_custom_location.db';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que no primeiro schema a localização é a default **`dbfs:/user/hive/warehouse/`** e que no segundo schema o diretório é o nome do schema com a extensão **`.db`**

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que a localização do segundo schema é o diretório especificado depois da palavra chave **`LOCATION`**

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos criar uma tabela nos schema com location padrão e inserir dados.
-- MAGIC 
-- MAGIC Observe que o schema deve ser fornecido.

-- COMMAND ----------

-- definir schema (schema sem a localização)
USE ${da.schema_name}_default_location;

-- criar tabela
CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);

-- inserir valores
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Podemos examinar a descrição da tabela para encontrar o local.

-- COMMAND ----------

DESCRIBE DETAIL managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Por padrão, tabelas gerencidas em um sxchema sem a localização especificada será criada no diretório **`dbfs:/user/hive/warehouse/<schema_name>.db/`** 
-- MAGIC 
-- MAGIC Podemos ver que, como esperado, os dados e metadados da nossa tabela Delta são armazenados nesse local.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC table_name = "managed_table_in_db_with_default_location"
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL {table_name}").first().location
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dropar a tabela.

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que o diretório da tabela e seus logs e arquivos de dados são deletados. Somente o diretório do schema permanece.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
-- MAGIC print(schema_default_location)
-- MAGIC dbutils.fs.ls(schema_default_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos criar uma tabela no schema com a localização personalizada.
-- MAGIC 
-- MAGIC Observe que o schema deve ser fornecido.

-- COMMAND ----------

-- definir schema (schema com localização)
USE ${da.schema_name}_custom_location;

-- criar tabela
CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);

-- inserir dados
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos olhar a descrição da tabela para encontrar a localização

-- COMMAND ----------

DESCRIBE DETAIL managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como esperado, esta tabela gerenciada é criada na path especificada com a palavra chave **`LOCATION`** durante a criação do schema. Como tal, o dado e metadado da tabela são persistidos no diretório.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC table_name = "managed_table_in_db_with_custom_location"
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL {table_name}").first().location
-- MAGIC print(tbl_location)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- dropar a tabela
DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que a pasta da tabela, logs de arquivos e os arquivos de dados são deletados. Somente o schema permanece.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema_custom_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_custom_location").collect()[3].database_description_value
-- MAGIC print(schema_custom_location)
-- MAGIC 
-- MAGIC dbutils.fs.ls(schema_custom_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tabelas
-- MAGIC Iremos criar uma tabela externa a partir dos dados de amostra.
-- MAGIC 
-- MAGIC Os dados que vamos usar são no formato CSV. Queremos criar uma tabela Delta com uma **`LOCATION`** fornecida no diretório da nossa escolha.

-- COMMAND ----------

-- definir schema
USE ${da.schema_name}_default_location;

-- criar uma temporary view
CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);

-- criar uma tabela a partir da temp view
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;
  
SELECT * FROM external_table; 

-- COMMAND ----------

-- olhar a location da tabela
DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- dropar a tabela
DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A definição da tabela não existe mais no metastore, mas os dados subjacentes permanecem intactos.

-- COMMAND ----------

-- dropar os schemas
DROP SCHEMA ${da.schema_name}_default_location CASCADE;
DROP SCHEMA ${da.schema_name}_custom_location CASCADE;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()
