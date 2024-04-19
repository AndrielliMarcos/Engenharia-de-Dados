# Databricks notebook source
from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

demo_df = race_results_df.filter('race_year = 2020')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### count, countDistinct, sum

# COMMAND ----------

# contar todos os registros
demo_df.select(count('*')).show()

# COMMAND ----------

# contar os registros de uma coluna específica
demo_df.select(count('race_name')).show()

# COMMAND ----------

# analisar o número único de corridas que ocorreram em 2020
demo_df.select(countDistinct('race_name')).show()

# resultado: aconteceram 17 corridas em 2020

# COMMAND ----------

# total de pontos concedidos nas corridas
demo_df.select(sum('points')).show()

# COMMAND ----------

# total de pontos concedidos nas corridas por 1 piloto específico
demo_df.filter(" driver_name = 'Lewis Hamilton' ").select(sum('points')).show()

# COMMAND ----------

# total de pontos concedidos nas corridas por 1 piloto específico e o total de corridas
demo_df.filter(" driver_name = 'Lewis Hamilton' ").select(sum('points'), countDistinct('race_name')) \
    .withColumnRenamed('sum(points)', 'total_points') \
    .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races') \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### group by

# COMMAND ----------

# total de pontos e número de corridas de cada piloto
demo_df \
    .groupBy('driver_name') \
    .sum('points') \
    .show()

# COMMAND ----------

# para fazer mais de uma agregação, como fizemos na célula 11, é necessário usar o agg()
demo_df \
    .groupBy('driver_name') \
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races')) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window Function

# COMMAND ----------

demo_df = race_results_df.filter('race_year in (2019, 2020)')

# COMMAND ----------

display(demo_df)

# COMMAND ----------

# agrupar por 2 colunas
demo_grouped_df = demo_df \
    .groupBy('race_year', 'driver_name') \
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('number_of_races'))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

# criar uma nova coluna e chamar de classificação com base no total de pontos na ordem decrescente. Os dados serão perticionados por ano, e a ordem será por total de pontos na ordem decrescente

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driveRanck = Window.partitionBy('race_year').orderBy(desc('total_points'))
demo_grouped_df.withColumn('rank', rank().over(driveRanck)).show()

# COMMAND ----------

