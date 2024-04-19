# Databricks notebook source
# MAGIC %md
# MAGIC ###Acessar um dataframe usando linguagem SQL
# MAGIC ##### Objetivos
# MAGIC 1. Criar temporary views a partir de um dataframe
# MAGIC 2. Acessar a view de uma célula SQL
# MAGIC 3. Acessar a view de uma célula python
# MAGIC
# MAGIC **Observação:** as temp views só são válidas em uma sessão do Spark, ou seja, elas não ficam disponíveis para outros notebooks acessarem.

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# criar uma temp view
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC --consultar a temp view
# MAGIC SELECT *
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

# consultar a temp view com pyspark
race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temporary Views

# COMMAND ----------

# criar uma temp view global
race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()