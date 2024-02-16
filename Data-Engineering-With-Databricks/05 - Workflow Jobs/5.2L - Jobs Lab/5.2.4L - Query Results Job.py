# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-05.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC #Explorando os resultados do pipeline DLT
# MAGIC Execute a célula a seguir para enumerar a saída de seu loval de armazenamento.

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC O diretório **system** captura eventos associados com o pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Este logs de evento são armazenados como Delta table.
# MAGIC 
# MAGIC Vamos consultar a tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos ver o conteúdo do diretório *tables*.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos consultar a tabela gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${DA.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()
