# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestão do arquivo lap_times_folder

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
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),    
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),    
    StructField('millisegundos', IntegerType(), True)
])

# COMMAND ----------

# DBTITLE 1,Leitura dos dados na camada Raw
# na path vamos especificar somente o nome da pasta onde os arquivos estão 
lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# DBTITLE 1,Renomear colunas existentes e incluir nova coluna
lap_times_final_df = add_ingestion_date(lap_times_df) \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
# lap_times_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Delta Lake
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success!')