# Databricks notebook source
# MAGIC %md
# MAGIC #### Classificações das equipes

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

# DBTITLE 1,Definir a data do arquivo
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Identificar o ano para o qual os dados precisam ser reprocessados

# COMMAND ----------

# DBTITLE 1,Leitura dos dados sobre o resultado das corridas
race_result_df = spark.read.format('delta') \
.load(f'{presentation_folder_path}/race_results') \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_result_df, 'race_year')

# COMMAND ----------

race_result_df = spark.read.format('delta') \
.load(f'{presentation_folder_path}/race_results') \
.filter(col('race_year').isin(race_year_list))

# COMMAND ----------

# DBTITLE 1,Agregação dos dados
constructor_standings_df = race_result_df \
    .groupBy('race_year', 'team') \
    .agg(sum('points').alias('total_points'),
         count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

# DBTITLE 1,Classificação das equipes
constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructor_standings_df.withColumn('rank', rank().over(constructor_rank_spec))

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Data Lake
# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Delta Lake
merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from f1_presentation.constructor_standings where race_year = 2021