# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

# escolher as corridas de 2019 e os registros das 5 primeiras rodadas
# filter = where

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")
# races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <=5))
# races_filtered_df = races_df.where((races_df["race_year"] == 2019) & (races_df["round"] <=5))

# COMMAND ----------

display(races_filtered_df)