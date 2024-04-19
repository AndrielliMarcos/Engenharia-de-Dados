# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestão do arquivo races.csv

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, concat, lit


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
races_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('year', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('circuitId', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('date', DateType(), True),
    StructField('time', StringType(), True),
    StructField('url', StringType(), True)
]) 

# COMMAND ----------

# DBTITLE 1,Leitura dos dados na camada Raw
races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# DBTITLE 1,Renomear as colunas
races_renamed_df = races_df.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('year', 'race_year') \
.withColumnRenamed('circuitId', 'circuit_id')


# COMMAND ----------

# DBTITLE 1,Inserir novas colunas
# inserir a coluna ingestion_date
# inserir a coluna race_timestamp (concatenar as colunas date e time)
# inserir a coluna data_source e configurar com o valor da variável v_data_source
# inserir a coluna file_date e configurar com o valor da variável v_file_date

races_new_colum_df = add_ingestion_date(races_renamed_df) \
    .withColumn('race_timestamp', to_timestamp( concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss') ) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# DBTITLE 1,Selecionar colunas necessárias
races_final_df = races_new_colum_df.select(
    col('race_id'), 
    col('race_year'), 
    col('round'), 
    col('circuit_id'), 
    col('name'),
    col('data_source'),
    col('race_timestamp'), 
    col('ingestion_date'),
    col('file_date')
)

# COMMAND ----------

# DBTITLE 1,Gravar os dados no Delta Lake
# o particionamento será feito por ano. E o modo de gravação será o overwrite
races_final_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')


# COMMAND ----------

# DBTITLE 1,Leitura do arquivo gravado
# display(spark.read.parquet(f'{processed_folder_path}/races'))

# COMMAND ----------

dbutils.notebook.exit('Success!')