# Databricks notebook source
# MAGIC %md
# MAGIC #### Acessar Azure Data Lake usando SAS token
# MAGIC 1. Especificar a configuração spark para SAS token
# MAGIC 1. Listar os arquivos do container 'demo'
# MAGIC 1. Ler os dados do arquivo 'circuits.csv'
# MAGIC
# MAGIC **Observação:** as assinaturas de acesso compartilhado podem ser usadas para controlar o acesso em um nível mais granular. Podemos restringir o acesso a tipos de recursos ou serviços específicos. Por exemplo, podemos permitir o acessi apenas a contêineres do Blob Storage, restringindo assim o acesso a arquivos, tabelas, entre outros. Além disso, podemos permitir o acesso somente de leitura a um contêiner, restringindo assim o usuário de gravar ou excluir pastas ou contêiners.

# COMMAND ----------

# config spark
spark.conf.set("fs.azure.account.auth.type.stgandriellimarcos.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.stgandriellimarcos.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.stgandriellimarcos.dfs.core.windows.net", 'sp=rl&st=2024-02-27T18:56:05Z&se=2024-02-28T02:56:05Z&spr=https&sv=2022-11-02&sr=d&sig=gPD0Nvt4zEUAGZavNfeYPJxw6QUBzsdMw1QvScXfhJw%3D&sdd=1')


# COMMAND ----------

# listar arquivos
display(dbutils.fs.ls("abfss://formula1-project@stgandriellimarcos.dfs.core.windows.net/demo"))

# COMMAND ----------

# ler os dados do aquivo circuits.csv
display(spark.read.csv("abfss://formula1-project@stgandriellimarcos.dfs.core.windows.net/demo/circuits.csv"))