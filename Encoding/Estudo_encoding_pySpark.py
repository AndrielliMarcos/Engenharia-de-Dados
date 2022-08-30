# Databricks notebook source
# MAGIC %md
# MAGIC ###Este notebook tem como objetivo identificar e setar o encoding de um arquivo.
# MAGIC ###Este estudo surgiu da necessidade de trabalhar com campos onde havia textos com caracteres especiais.
# MAGIC ###Em um primeiro momento foi feito o replace nos caracteres especiais, mas depois de alguns estudos foi feito o refatoramento do código.
# MAGIC ###O código refatorado está nas células abaixo.
# MAGIC 
# MAGIC ###Passos a seguir:
# MAGIC 1. fazer a conexão com o Data Lake
# MAGIC 2. verificar encoding do arquivo
# MAGIC 3. ler o arquivo do Data Lake
# MAGIC 4. selecionar as colunas necessárias
# MAGIC 5. fazer tratamento, caso seja necessário
# MAGIC 6. gravar no Data Lake com formato parquet

# COMMAND ----------

# DBTITLE 1,Bibliotecas
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Variáveis de conexão
# variáveis para conexão com o Data Lake
storage_account_name = 'stgandriellimarcos'
scope_name           = 'dbw-scope-andrielli-marcos'
blobKey              = 'blob-key'
containers           = ['transient', 'estudos']

# COMMAND ----------

# DBTITLE 1,Montar a conexão com Data Lake
config = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":dbutils.secrets.get(scope = f"{scope_name}", key = f"{blobKey}")}

for container in containers:
    dbutils.fs.mount(
                    source = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net",
                    mount_point = f"/mnt/{container}",
                    extra_configs = config
                )

# dbutils.fs.ls("/mnt/")

# COMMAND ----------

# DBTITLE 1,Desmontar a conexão com Data Lake
# dbutils.fs.unmount(f"/mnt/")

# COMMAND ----------

# DBTITLE 1,Identificar o encoding do arquivo
# MAGIC %sh file -bi /dbfs/mnt/transient/Lista_Estados_Brasil_Versao_CSV.csv

# COMMAND ----------

# DBTITLE 1,Leitura do CSV
# diretório de origem
path_file = '/mnt/transient/Lista_Estados_Brasil_Versao_CSV.csv'

df = spark.read.csv(
    path_file,
    header=True,
    sep=';',
    inferSchema=True,
    encoding='iso-8859-1'
)

display(df)

# COMMAND ----------

# DBTITLE 1,Dataframe com as colunas necessárias
df = df.select(
    col("Estado"),
    col("UF"),
    col("Região")
)

display(df)

# COMMAND ----------

# DBTITLE 1,Ordenar por Estado
df = df.orderBy("Estado")

display(df)

# COMMAND ----------

# DBTITLE 1,Gravar arquivo formato parquet
path_file_destino = '/mnt/estudos/Lista_Estados_Brasil_Versao_PARQUET'

df.write\
      .mode('overwrite')\
      .format('parquet')\
      .save(path_file_destino)
