# Databricks notebook source
# MAGIC %md
# MAGIC #Explorando os resultados de um pipeline DLT
# MAGIC Embora o DLT abstraia muitas das complexidades associadas à execução de ETL de produção no Databricks, muitas pessoas podem se perguntar o que realmente está acontecendo nos bastidores.
# MAGIC 
# MAGIC Neste notebook, iremos explorar como os dados e os metadados são persistidos pelo DLT.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-04.3

# COMMAND ----------

# MAGIC %md
# MAGIC ###Consultando tabelas no database destino
# MAGIC Desde que um banco de dados de destino seja especificado durante a configuração do DTL pipeline, as tabelas devem estar disponíveis para os usuários em todo o ambiente Databricks.
# MAGIC 
# MAGIC Execute a célula abaixo para ver as tabelas cadastradas no banco de dados utilizado nesta aula.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${DA.schema_name};
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC Observe que a view que definimos em nosso pipeline está presente em nossa lista de tabelas.
# MAGIC 
# MAGIC Resultados da consulta da tabela **`orders_bronze`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Lembre-se de que **`orders_bronze`** foi definida como uma streaming live table em DLT, mas nossos resultados aqui são estáticos.
# MAGIC 
# MAGIC Como o DLT usa o Delta Lake para armazenar todas as tabelas, cada vez que uma consulta é executada, sempre retornaremos a versão mais recente da tabela. Mas as consultas fora do DTL retornarão resultados instantâneos das tabelas DLT, independentemente de como eles foram definidos.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Examinar os resultados de **`APPLY CHANGES INTO`**
# MAGIC Lembre-se que a tabela **`customers_silver`** foi implementada com alterações de um feed CDC aplicado como Tipo SCD.
# MAGIC 
# MAGIC Vamos consultar esta tabela abaixo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Observe que a tabela **`customers_silver`** representa corretamente o estado ativo atual de nossa tabelas Tipo 1 com alterações aplicadas, mas não inclui os campos adicionais vistos no esquema mostrado na interface do usuário DLT: **__Timestamp**, **__DeleteVersion**, e **__UpsertVersion**.
# MAGIC 
# MAGIC Isso ocorre porque nossa tabela **`customers_silver`** é realmente implementada como uma view em uma tabela oculta chamada **`__apply_changes_storage_customers_silver`**
# MAGIC 
# MAGIC Podemos ver isso se executarmos **`DESCRIBE EXTENDED`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Se consultarmos esta tabela oculta, veremos estes 3 campos. No entanto, os usuários não precisam interagir diretamente com esta tabela, pois ela é apenas aproveitada pelo DLT para garantir que as atualizações sejam aplicadas na ordem correta para materializar os resultados corretamente.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM __apply_changes_storage_customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ###Examinando arquivos de dados
# MAGIC Execute a célula a seguir para ver os arquivos configurados no **Storage location**

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Os diretórios **autoloader** e **checkpoint** contêm dados usados para gerenciar o processamento de dados incrementais com streaming estruturado.
# MAGIC 
# MAGIC O diretório **system** captura eventos associados com o pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Estes logs de evento são armazenados como tabela Delta. Vamos consultar a tabela.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{DA.paths.storage_location}/system/events`"))

# COMMAND ----------

# conteúdo do diretório de tabelas
files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Cada um desses diretórios contém uma tabela Delta Lake sendo gerenciada pelo DLT.
