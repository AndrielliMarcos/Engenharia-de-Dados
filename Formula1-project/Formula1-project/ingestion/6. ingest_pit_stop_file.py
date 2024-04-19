# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestão do arquivo pit_stops.json

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
pit_stops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', StringType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

# DBTITLE 1,Leitura dos dados na camada Raw
# option('multiline', True) => usado para especificar que o arquivo é de várias linhas
pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option('multiline', True) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# DBTITLE 1,Renomear colunas existentes e incluir nova coluna
pit_stops_final_df = add_ingestion_date(pit_stops_df) \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
#pit_stops_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stop')

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Delta Lake
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success!')