# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestão do arquivo constructors.json

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Executar o notebook 'configuration'
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Executar o notebook 'common_functions'
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# DBTITLE 1,Criar uma variável em tempo de execução
dbutils.widgets.text("p_data_source", "")
dbutils.widgets.text("p_file_date", "2021-03-21")

# COMMAND ----------

# DBTITLE 1,Recuperar o valor atribuído a variável
v_data_source = dbutils.widgets.get("p_data_source")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Definir schema
# Esta é outra opção de declarar o schema usando DDL ao invés de StructType() e StructField()
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# DBTITLE 1,Leitura dos dados na camada Raw
constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# DBTITLE 1,Excluir as colunas desnecessárias
constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# DBTITLE 1,Renomear colunas existentes e incluir novas colunas
constructor_final_df = add_ingestion_date(constructor_dropped_df) \
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef', 'constructor_ref') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
constructor_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/constructors'))

# COMMAND ----------

dbutils.notebook.exit('Success!')