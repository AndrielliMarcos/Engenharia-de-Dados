# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake
# MAGIC 1. Especificar a configuração spark fs.azure.account.key
# MAGIC 1. Configurar o Spark Config com as credenciais do Key Vault
# MAGIC 1. Montar o Storage com o *mount*
# MAGIC 1. Listar todos os *mounts* e desmontar o Storage com o *unmount*

# COMMAND ----------

# listar escopos do Databricks
# dbutils.secrets.listScopes()

# listar as chaves de um escopo
# dbutils.secrets.list(scope='formula1-project-scope')

# COMMAND ----------

# variáveis para conexão com o Data Lake
storage_account_name = 'stgandriellimarcos'
scope_name           = 'formula1-project-scope' # nome dados ao criar o scipe no Databricks
blobKey              = 'formula1-project-key' # access key do blob storage guardada no key 
container            = 'formula1-project'

# COMMAND ----------

# Conexão com o Data Lake
config = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":dbutils.secrets.get(scope = f"{scope_name}", key = f"{blobKey}")}

# COMMAND ----------

# Monta Camada no DBFS
dbutils.fs.mount(
                source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
                mount_point = f"/mnt/{container}",
                extra_configs = config
            )

# COMMAND ----------

# list diretórios e arquivos
display(dbutils.fs.ls("mnt/formula1-project/demo"))

# COMMAND ----------

# ler os dados do aquivo circuits.csv
display(spark.read.csv("/mnt/formula1-project/demo/circuits.csv"))

# COMMAND ----------

# listar todos os mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# remover o mount
# dbutils.fs.unmount('/mnt/formula1-project/')