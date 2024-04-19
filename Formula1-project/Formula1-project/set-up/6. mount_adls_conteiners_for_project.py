# Databricks notebook source
# MAGIC %md
# MAGIC **Obs:** O mount será criado para cada container que for passado por parâmetro. No caso de ter mais conteiners, pode-se criar uma lista com os nomes e percorrer essa lista para montar um mount para cada conteiner.

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # variáveis para conexão com o Data Lake
    scope_name           = 'formula1-project-scope' # nome dados ao criar o scipe no Databricks
    blobKey              = 'formula1-project-key' # access key do blob storage guardada no key 
    
    # Conexão com o Data Lake
    config = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net":dbutils.secrets.get(scope = f"{scope_name}", key = f"{blobKey}")}

    # Verificar se o mount existe. Se existir ele será desmontado e montado novamente
    if any (mount.mountPoint == f"/mnt/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{container_name}")

    # Monta Camada no DBFS
    dbutils.fs.mount(
                    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
                    mount_point = f"/mnt/{container_name}",
                    extra_configs = config
                )
    # listar os mounts criados
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('stgandriellimarcos', 'formula1-project')

# COMMAND ----------

# list diretórios e arquivos
display(dbutils.fs.ls("mnt/formula1-project/"))