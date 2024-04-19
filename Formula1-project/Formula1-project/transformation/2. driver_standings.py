# Databricks notebook source
# MAGIC %md
# MAGIC #### Classificações dos pilotos
# MAGIC Obter a classificação dos pilotos no ano da corrida.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import sum, count, when, col, desc, rank
from pyspark.sql.window import Window

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

# MAGIC %md
# MAGIC Identificar o ano para o qual os dados precisam ser reprocessados

# COMMAND ----------

# DBTITLE 1,Leitura dos dados sobre o resultado das corridas
# filter(f"file_date = '{v_file_date}' ") => irá retornar todos os dados dos arquivos desta data
# select('race_year') => obter o ano distinto da corrida
# distinct() => retornar todos os anos de corridas distintas
# collect() => transforma em uma lista
race_results_list = spark.read.format('delta') \
    .load(f'{presentation_folder_path}/race_results') \
    .filter(f"file_date = '{v_file_date}' ") 

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, 'race_year')

# COMMAND ----------

# os dados serão filtrados pelo ano da corrida
race_result_df = spark.read.format('delta') \
    .load(f'{presentation_folder_path}/race_results') \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# DBTITLE 1,Agregação dos dados
# soma dos pontos, de acordo com as colunas agrupadas
# contagem da posição, quando esta for = 1 (isso significa que será contado quantas vezes o piloto venceu a corrida)
# o resultado da agregação será os pontos por ano
# a equipe não estrará, já que um piloto pode mudar de equipe durante uma temporada

driver_standings_df = race_result_df \
    .groupBy('race_year', 'driver_name', 'driver_nationality') \
    .agg(sum('points').alias('total_points'),
         count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

# DBTITLE 1,Classificação dos pilotos
# se o total de pontos empatar, a classificação será feita pelo número de vitórias
driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Delta Lake
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from f1_presentation.driver_standings where race_year = 2021

# COMMAND ----------

# MAGIC %sql
# MAGIC -- contagem do número total de registros para cada um dos anos da corrida
# MAGIC -- select race_year, count(1)
# MAGIC -- from f1_presentation.driver_standings
# MAGIC -- group by race_year
# MAGIC -- order by race_year desc;