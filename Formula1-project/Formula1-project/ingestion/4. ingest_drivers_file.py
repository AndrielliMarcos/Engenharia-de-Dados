# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestão do arquivo drivers.json

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

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

# MAGIC %md
# MAGIC ##### Definir Schema

# COMMAND ----------

# MAGIC %md
# MAGIC Para este arquivo, será necessário definir dois schemas para facilitar a leitura. No campo nome temos um objeto JSON ('forname' e 'surname'), que está dentro do JSON externo (campo aninhado). 
# MAGIC
# MAGIC Primeiro será definido o JSON interno, e depois ele será envolvido no objeto externo.

# COMMAND ----------

# DBTITLE 1,Definir schema do campo name 
name_schema = StructType(fields=[
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Definir schema
# no StructField do campo 'name' o tipo será o schema configurado acima
drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Leitura dos dados na camada Raw
drivers_df = spark.read\
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# DBTITLE 1,Renomear colunas existentes e incluir novas colunas
drivers_with_columns_df = add_ingestion_date(drivers_df) \
                                    .withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('driverRef', 'driver_ref') \
                                    .withColumn('data_source', lit(v_data_source)) \
                                    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                    .withColumn('file_date', lit(v_file_date ))
                                                       

# COMMAND ----------

# DBTITLE 1,Excluir as colunas desnecessárias
drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable('f1_processed.drivers')

# COMMAND ----------

# display(spark.read.parquet(f'{processed_folder_path}/drivers'))

# COMMAND ----------

dbutils.notebook.exit('Success!')