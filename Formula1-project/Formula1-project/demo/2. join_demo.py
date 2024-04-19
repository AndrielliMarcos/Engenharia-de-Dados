# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### circuits e races(corridas) => circuit_id

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Inner Join

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').filter("race_year = 2019") \
    .withColumnRenamed('name', 'race_name')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# unir os 2 dataframes
# inner => pega os dados da tabela circuits_df e une com os dados da tabela circuits_df, mas somente onde o campo circuit_id for igual
# podemos observar no resultado que nem toda corrida teve um circuito, mas os 21 circuitos receberam 1 corrida
# ourea observação é que o dataframe restornado contem todos os campos das 2 tabelas que estão sendo unidas. Podemos usar o SELECT para obter somente as colunas desejadas.
# agora temos 2 colunas chamadas 'name', já que cada uma vem de uma tabela. Então vamos renomear estas colunas antes de fazer o join
  
race_circuits_df = circuits_df.join(races_df, 
                                    circuits_df.circuit_id == races_df.circuit_id, 'inner') \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Outer Joins

# COMMAND ----------

# vamos excluir alguns circuitos para que tenhamos algumas corridas sem os circuitos

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits') \
    .filter('circuit_id < 70') \
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

# Left outer join
race_circuits_df = circuits_df.join(races_df, 
                                    circuits_df.circuit_id == races_df.circuit_id, 'left') \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# right outer join
race_circuits_df = circuits_df.join(races_df, 
                                    circuits_df.circuit_id == races_df.circuit_id, 'right') \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# full outer join
race_circuits_df = circuits_df.join(races_df, 
                                    circuits_df.circuit_id == races_df.circuit_id, 'full') \
                    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Joins

# COMMAND ----------

# retorna os dados do dataframe da esquerda, como no left join
# podendo usar o SELECT para selecionar as colunas desejadas
race_circuits_df = circuits_df.join(races_df, 
                                    circuits_df.circuit_id == races_df.circuit_id, 'semi')

# COMMAND ----------

display(race_circuits_df)