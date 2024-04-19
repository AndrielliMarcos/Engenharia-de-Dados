# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestão do arquivo results.json

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp, col, lit

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
results_schema = StructType(fields=[
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), True),
    StructField('positionOrder', IntegerType(), True),
    StructField('points', FloatType(), True),
    StructField('laps', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLamp', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLampTime', StringType(), True),
    StructField('fastestLampSpeed', FloatType(), True),
    StructField('statusId', StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Leitura dos dados na camada Raw
results_df = spark.read\
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# DBTITLE 1,Excluir as colunas desnecessárias
results_dropped_df = results_df.drop(col('statusId'))

# COMMAND ----------

# DBTITLE 1,Renomear colunas existentes e incluir novas colunas
results_final_df = add_ingestion_date(results_dropped_df) \
    .withColumnRenamed('resultId', 'result_id')\
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('positionText', 'position_text') \
    .withColumnRenamed('positionOrder', 'position_order') \
    .withColumnRenamed('fastestLamp', 'fastest_lamp') \
    .withColumnRenamed('fastestLampTime', 'fastest_lamp_time') \
    .withColumnRenamed('fastestLampSpeed', 'fastest_lamp_speed') \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))    
    

# COMMAND ----------

# DBTITLE 1,Deletar dados duplicados
results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
# o particionamento será feito por race_id
#results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Delta Lake
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT race_id, driver_id, count(1)
# MAGIC -- FROM f1_processed.results
# MAGIC -- GROUP BY race_id, driver_id
# MAGIC -- HAVING count(1) > 1
# MAGIC -- ORDER BY race_id, driver_id DESC

# COMMAND ----------

dbutils.notebook.exit('Success!')