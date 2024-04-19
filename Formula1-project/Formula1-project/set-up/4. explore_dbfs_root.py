# Databricks notebook source
# MAGIC %md
# MAGIC #### Explorando o DBFS Root
# MAGIC 1. Listar todas as pastas no DBFS Root
# MAGIC 2. Interagir com DBFS File Browser
# MAGIC 3. Carregar arquivos do DBFS Root

# COMMAND ----------

# listar diretórios criados por padrão no DBFS
display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %md
# MAGIC Existe uma pasta chamada FileStore, na qual podemos fazer upload de arquivos usando a UI do Databricks. Para usar esta pasta ela deve estar habilitada. Basta conferir no Console Admin.

# COMMAND ----------

# mostrar arquivos do diretório FileStore
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# leitura do arquivo circuits.csv
display(spark.read.csv('/FileStore/circuits.csv'))