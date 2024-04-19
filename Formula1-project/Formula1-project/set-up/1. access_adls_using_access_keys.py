# Databricks notebook source
# MAGIC %md
# MAGIC #### Acessar Azure Data Lake usando access keys
# MAGIC 1. Especificar a configuração spark fs.azure.account.key
# MAGIC 1. Listar os arquivos do container 'demo'
# MAGIC 1. Ler os dados do arquivo 'circuits.csv'
# MAGIC
# MAGIC **Observação:** O problema com as chaves de acesso é que elas dão acesso total a toda a conta de armazenamento.

# COMMAND ----------

# scope do databricks vinculado ao Key Vault
formula1_account_key = dbutils.secrets.get(scope='formula1-project-scope', key='formula1-project-key')

# COMMAND ----------

# config spark
spark.conf.set(
    "fs.azure.account.key.stgandriellimarcos.dfs.core.windows.net",
    formula1_account_key
)

# COMMAND ----------

# listar arquivos
display(dbutils.fs.ls("abfss://formula1-project@stgandriellimarcos.dfs.core.windows.net/demo"))

# COMMAND ----------

# ler os dados do aquivo circuits.csv
display(spark.read.csv("abfss://formula1-project@stgandriellimarcos.dfs.core.windows.net/demo/circuits.csv"))