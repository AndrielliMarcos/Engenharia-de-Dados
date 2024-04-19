# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore os recursos do dbutils.secrets

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-project-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-project-scope', key='formula1-project-key')