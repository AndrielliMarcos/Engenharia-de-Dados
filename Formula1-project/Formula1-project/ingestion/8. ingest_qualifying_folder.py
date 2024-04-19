# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestão do arquivo qualifying_folder

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
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
lap_times_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),    
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),    
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Leitura dos dados na camada Raw
# option('multiline', True) => usado para especificar que o arquivo é de várias linhas
# na path vamos especificar somente o nome da pasta onde os arquivos estão 
qualifying_df = spark.read \
    .schema(lap_times_schema) \
    .option('multiline', True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# DBTITLE 1,Renomear colunas existentes e incluir novas colunas
qualifying_final_df = add_ingestion_date(qualifying_df) \
    .withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
# qualifying_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.qualify_id = src.qualify_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success!')