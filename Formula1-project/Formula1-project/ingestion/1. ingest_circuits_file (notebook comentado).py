# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingestão do arquivo circuits.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1º passo: Ler o arquivo CSV usando Spark Dataframe Reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Verificar os mounts montados

# display(dbutils.fs.mounts())

# COMMAND ----------

# Verificar as paths do arquivo que iremos trabalhar (circuits.csv)

# display(dbutils.fs.ls('/mnt/formula1-project/')) 

# COMMAND ----------

# criar uma variável usando o widgets para configurar a data atual que o arquivo está sendo ingerido
dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

# recuperar o valor atribuído a variável
v_data_source = dbutils.widgets.get("p_data_source")
# v_data_source

# COMMAND ----------

# criar uma variável usando o widgets para configurar a data do arquivo
# valor padrão configurado com a primeira data dos arquivos
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
# v_file_date

# COMMAND ----------

# leitura do arquivo circuits.csv
# header = True => identifica o cabeçalho do arquivo
# inferSchema = True => analisa os dados e infere o schema

# circuits_df = spark.read \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .csv('dbfs:/mnt/formula1-project/raw/circuits.csv')

# COMMAND ----------

# leitura do arquivo sem a option inferSchema

# circuits_df = spark.read \
#     .option("header", True) \
#     .csv('dbfs:/mnt/formula1-project/raw/circuits.csv')

# COMMAND ----------

# outra forma de identificar/confirmar o tipo de dados de cada coluna é usando o describe()
# ele retorna alguns dados que podemos usar para confirmar o tipo
 
# circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Observe que na leitura com a option inferSchema há 2 Spark Jobs. Já na leitura sem a option inferSchema há 1 Spark Job. Ou seja, quando o inferSchema é executado como True, o Spark percorre os dados, lê eles e identifica qual deve ser o schema e, em seguida, aplica esse schema ao dataframe. Note que nesse caso ele lê todos os dados.
# MAGIC
# MAGIC Em um ambiente de produção serão muitos dados, o que pode tornar as leituras mais lentas. Além disso, se o desenvolvedor obtiver dados que não confirmam o que ele espera, o processo deve falhar e informar que há algo errado, em vez de apenas inferir o schema e continuar.
# MAGIC
# MAGIC Portanto, a option inferSchema pode ser executada como true se o desenvolvedor estiver fazendo testes de esforço e, às vezes, também pode ser usado se a quantidadede dados for muito pequena. Mas para trabalhar em um projeto de nível de produção e também com uma grande quantidade de dados, é necessário especificar o schema e pedir ao Spark para usá-lo. E se os dados não estiverem de acordo com o schema, o processo deve falhar.
# MAGIC
# MAGIC Para especificar o schema, é necessário entender os tipos de dados do DataFrame.

# COMMAND ----------

# importar os tipos
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


# COMMAND ----------

# definir schema
# StructField(coluna, tipo, é null ou não)
circuits_schema = StructType(fields=[
    StructField('circuitId', IntegerType(), False),
    StructField('circuitRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('location', StringType(), True),
    StructField('country', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('lng', DoubleType(), True),
    StructField('alt', IntegerType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

# leitura do arquivo sem a option inferSchema e com a definição do schema
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# mostrar os dados

#display(circuits_df)

# COMMAND ----------

# verificar o schema do arquivo
# retorna o nome das colunas e o datatype

#circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2º passo: Selecionar somente as colunas necessárias

# COMMAND ----------

# selecionar a colunas usando somente o select()
circuits_selected_df = circuits_df.select('circuitId', 'circuitRef', 'name', 'location',  'country', 'lat', 'lng', 'alt')

# COMMAND ----------

# selecionar a colunas usando select() e col()

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3º passo: Renomear algumas colunas

# COMMAND ----------

# vamos adicionar uma coluna com o nome da fonte de dados que foi configurado no widget
circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId', 'circuit_id') \
    .withColumnRenamed('circuitRef', 'circuit_ref') \
    .withColumnRenamed('lat', 'latitude') \
    .withColumnRenamed('lng', 'longitude') \
    .withColumnRenamed('alt', 'altitude') 

# COMMAND ----------

# display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4º passo: Adicionar novas colunas e preencher com a data atual e a data do arquivo
# MAGIC
# MAGIC

# COMMAND ----------

# circuits_final_df = add_ingestion_date(circuits_renamed_df)
circuits_final_df = add_ingestion_date(circuits_renamed_df) \
                    .withColumn('data_source', lit(v_data_source)) \
                    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# display(circuits_final_df)

# COMMAND ----------

# se quisessemos adicionar uma coluna e preencher todas as linhas com o mesmo valor

# circuits_final_df_new = circuits_renamed_df.withColumn("env", lit("Producton"))

# COMMAND ----------

# display(circuits_final_df_new)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### 5º passo: Salvar os dados no Data Lake no formato parquet

# COMMAND ----------

# mode("overwrite") => os dados serão sobrescritos sempre que esse comando for executado
# os dados serão salvos na tabela 'f1_processed.circuits', que é criada em tempo de execução com este comando
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# leitura do arquivo parquet
# display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit('Success!')