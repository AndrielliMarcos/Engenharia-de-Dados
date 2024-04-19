# Databricks notebook source
# MAGIC %md
# MAGIC #### Resultado das corridas

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Executar o notebook 'configuration'
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Executar o notebook 'common_functions'
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# DBTITLE 1,Variável criada em tempo de execução
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Leitura dos dados de corrida (races)
races_df = spark.read.format('delta').load(f'{processed_folder_path}/races') \
    .withColumnRenamed('circuit_id', 'race_circuit_id') \
    .withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date') \
    .select('race_id', 'race_circuit_id', 'race_year', 'race_name', 'race_date')    

# COMMAND ----------

# DBTITLE 1,Leitura dos dados dos circuitos (circuits)
circuits_df = spark.read.format('delta').load(f'{processed_folder_path}/circuits') \
                        .withColumnRenamed('location', 'circuit_location') \
                        .select('circuit_id', 'circuit_location')

# COMMAND ----------

# DBTITLE 1,Leitura dos dados dos pilotos (drivers)
drivers_df = spark.read.format('delta').load(f'{processed_folder_path}/drivers') \
                        .withColumnRenamed('name', 'driver_name') \
                        .withColumnRenamed('number', 'driver_number') \
                        .withColumnRenamed('nationality', 'driver_nationality') \
                        .select('driver_id', 'driver_name', 'driver_number', 'driver_nationality')

# COMMAND ----------

# DBTITLE 1,Leitura dos dados das equipes (constructors)
constructors_df = spark.read.format('delta').load(f'{processed_folder_path}/constructors') \
                            .withColumnRenamed('name', 'team') \
                            .select('constructor_id', 'team')

# COMMAND ----------

# DBTITLE 1,Leitura dos dados sobre os resultados (results)
results_df = spark.read.format('delta').load(f'{processed_folder_path}/results') \
                        .filter(f"file_date = '{v_file_date}' ") \
                        .withColumnRenamed('time', 'race_time') \
                        .withColumnRenamed('race_id', 'result_race_id') \
                        .withColumnRenamed('file_date', 'result_file_date') \
                        .select('result_race_id', 'driver_id', 'constructor_id', 'grid','fastest_lamp', 'race_time', 'points', 'position', 'result_file_date') 

# COMMAND ----------

# MAGIC %md
# MAGIC #####Joins

# COMMAND ----------

# DBTITLE 1,Join de corridas (races) com circuitos (circuits) 
races_circuits_df = races_df.join(circuits_df,
                                  races_df.race_circuit_id == circuits_df.circuit_id) \
                            .select(races_df.race_id, races_df.race_name, races_df.race_year, races_df.race_date, circuits_df.circuit_location)                            

# COMMAND ----------

# DBTITLE 1,Join dos resultados com os demais DataFrames
races_result_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

# DBTITLE 1,Selecionar colunas necessárias e incluir coluna
final_df = races_result_df.select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lamp', 'race_time', 'points', 'position', 'result_file_date') \
                            .withColumn('created_date', current_timestamp()) \
                            .withColumnRenamed('result_file_date', 'file_date')

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Delta Lake
# chave exclusiva desta tabela será a combinação no driver_name (o mais correto seroa o ID) e race_id 
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# DBTITLE 1,Contagem dos IDs das corridas
# MAGIC %sql
# MAGIC --select race_id, count(1) from f1_presentation.race_results
# MAGIC --group by	race_id
# MAGIC --order by race_id desc;