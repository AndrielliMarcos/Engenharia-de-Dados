# Databricks notebook source
# MAGIC %md
# MAGIC #### Fluxo de trabalho no Notebook

# COMMAND ----------

# verificar os comandos que podem ser configurados para executar outro notebook
# dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md 
# MAGIC Um notebook só será executado quando o anterior for finalizado.

# COMMAND ----------

v_result = dbutils.notebook.run('1. ingest_circuits_file (notebook comentado)', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('2. ingest_races_file', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('3. ingest_constructors_file', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('4. ingest_drivers_file', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('5. ingest_results_file', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('6. ingest_pit_stop_file', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('7. ingest_lap_times_file', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result

# COMMAND ----------

v_result = dbutils.notebook.run('8. ingest_qualifying_folder', 0, {'p_data_source': 'Ergast API'}, "p_file_date": "2021-03-21")
v_result