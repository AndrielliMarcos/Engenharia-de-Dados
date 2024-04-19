# Databricks notebook source
# MAGIC %md
# MAGIC ##### Objetivos:
# MAGIC 1. Salvar os dados no Delta Lake (managed table)
# MAGIC 1. Salvar os dados no Delta Lake (external table)
# MAGIC 1. Ler os dados do Delta Lake (tabela)
# MAGIC 1. Ler os dados do Delta Lake (arquivo) 

# COMMAND ----------

# DBTITLE 1,Criar um banco de dados
# MAGIC %sql
# MAGIC -- todas as tabelas gerenciadas serão criadas na location especificada
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1-project/demo/'

# COMMAND ----------

# DBTITLE 1,Leitura do arquivo results.json da camada raw
results_df = spark.read \
  .option('inferSchema', True) \
  .json('/mnt/formula1-project/raw/2021-03-28/results.json')

# COMMAND ----------

# DBTITLE 1,Salvar os dados em uma tabela gerenciada
# tabela gerenciada => saveAsTabele()
results_df.write.format('delta').mode('overwrite').saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# DBTITLE 1,Leitura dos dados diretamente na tabela
# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# DBTITLE 1,Salvar os dados em um local de arquivo
#  local de arquivo => save()
results_df.write.format('delta').mode('overwrite').save('/mnt/formula1-project/demo/results_external')

# COMMAND ----------

# DBTITLE 1,Criar uma external table para ler os dados
# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1-project/demo/results_external'

# COMMAND ----------

# DBTITLE 1,Leitura dos dados diretamente na tabela
# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external 

# COMMAND ----------

# DBTITLE 1,Ler os dados diretamente do Delta Lake
results_external_df = spark.read.format('delta').load('/mnt/formula1-project/demo/results_external')

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# DBTITLE 1,Gravar os dados de forma particionada
results_df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable('f1_demo.results_partitioned')

# COMMAND ----------

# DBTITLE 1,Mostrar as partições
# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Objetivos:
# MAGIC 1. Atualizar uma Delta Table
# MAGIC 1. Deletar dados da Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed; 

# COMMAND ----------

# DBTITLE 1,Atualizar a coluna 'points' usando SQL
# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed; 

# COMMAND ----------

# DBTITLE 1,Atualizar a coluna 'points' usando Python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1-project/demo/results_managed')

deltaTable.update('position <= 10', {'points': '21 - position'})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# DBTITLE 1,Excluir dados usando SQL
# MAGIC %sql
# MAGIC -- excluir os pilotos que não terminaram a corrida entre os 10 primeiros
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# DBTITLE 1,Excluir dados usando Python
# excluir dados dos piloros que obtiveram 0 pontos nas corridas
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1-project/demo/results_managed')

deltaTable.delete('points = 0')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upsert usando merge
# MAGIC Inserção ascendente em uma tabela Delta usando comandos Merge.
# MAGIC
# MAGIC Uma instrução **merge** permite inserir qualquer novo registro recebido. Atualizar todos os registros existentes para os quais foram recebidos nvos dados. E se existir uma solicitação de exclusão, poderá aplicá-la também. E esses 3 comandos podem ser feitos um uma única declaração.

# COMMAND ----------

# MAGIC %md
# MAGIC Simulação como se estivéssemos recebendo dados durante 3 dias seguidos

# COMMAND ----------

# dataFrame somente com os 10 primeiros pilotos
drivers_day1_df = spark.read \
.option("inferSchema", True)  \
.json('/mnt/formula1-project/raw/2021-03-28/drivers.json') \
.filter('driverId <= 10') \
.select('driverId', 'dob', 'name.forename', 'name.surname')

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

# DBTITLE 1,Criar tempview para trabalhar com SQL
drivers_day1_df.createOrReplaceTempView('drivers_day1')

# COMMAND ----------

# dataframe somente com os dados dos pilotos de 6 a 15
# de 1 a 5 será excluído
# de 6 a 10 será atualizado
# de 11 a 15 será inserido
from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True)  \
.json('/mnt/formula1-project/raw/2021-03-28/drivers.json') \
.filter('driverId BETWEEN 6 AND 15') \
.select('driverId', 'dob', upper('name.forename').alias('forename'), upper('name.surname').alias('surname'))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

# DBTITLE 1,Criar tempview para trabalhar com SQL
drivers_day2_df.createOrReplaceTempView('drivers_day2')

# COMMAND ----------

# dataframe somente com os dados dos pilotos de 1 A 5 ou de 16 a 20
# de 1 a 5 será atualizado
# de 6 a 15 será excluído
# de 16 a 20 será inserido
drivers_day3_df = spark.read \
.option("inferSchema", True)  \
.json('/mnt/formula1-project/raw/2021-03-28/drivers.json') \
.filter('driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20') \
.select('driverId', 'dob', upper('name.forename').alias('forename'), upper('name.surname').alias('surname'))

# COMMAND ----------

# DBTITLE 1,Criação da tabela Delta
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- f1_demo.drivers_merge => onde as alterações vão acontecer
# MAGIC -- drivers_day1 => vai usar esses dados para fazer as alterações
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd 
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd 
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1-project/demo/drivers_merge')

deltaTable.alias('tgt').merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId') \
.whenMatchedUpdate(set = {'dob': 'upd.dob', 'forename': 'upd.forename', 'surname': 'upd.surname', 'updatedDate': 'current_timestamp()'})\
.whenNotMatchedInsert(values= {
    'driverId': 'upd.driverId',
    'dob': 'upd.dob', 
    'forename': 'upd.forename', 
    'surname': 'upd.surname',
    'createdDate': 'current_timestamp()'
}) \
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Histórico dos dados e versionamento
# MAGIC 1. Time travel
# MAGIC 1. Vaccum

# COMMAND ----------

# DBTITLE 1,Histórico da tabela 'f1_demo.drivers_merge'
# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# DBTITLE 1,Visualizar os dados de acodso com a versão
# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1; 

# COMMAND ----------

# DBTITLE 1,Visualizar os dados de acordo com o TIMESTAMP
# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-03-13T13:18:01.000+00:00'; 

# COMMAND ----------

df = spark.read.format('delta').option('timestampAsOf', '2024-03-13T13:18:01.000+00:00').load('/mnt/formula1-project/demo/drivers_merge')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- vaccum remove completamente os dados do banco, inclusive o histórico, com mais de 7 dias (podendo ser alterado a quantidade de dias)
# MAGIC
# MAGIC -- para 7 dias
# MAGIC VACUUM f1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-03-13T13:18:01.000+00:00'; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- para remover os dados com menos de 7 dias
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-03-13T13:18:01.000+00:00'; 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Restaurar uma versão anterios dos dados

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC   ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Logs de Transação

# COMMAND ----------

# MAGIC %md
# MAGIC Para cada alteração que é feita nos dados, o log é armazenado em uma pasta no Data Lake (e não no Hive Metastore) chamada **_delta_log**. Esta pasta na mesma pasta que os arquivos estão armazenados.
# MAGIC
# MAGIC Para cada transação feita na tabela, é criado um arquivo .json onde é armazenada a transação feita. Quando buscamos o histórico das transações, ou selecionamos uma versão qualquer de uma tabela, os dados são para visualização são buscados dentro dos arquivos .json.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Converter Parquet para Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# DBTITLE 1,Converter uma tabela Parquet para Delta
# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;

# COMMAND ----------

df = spark.table('f1_demo.drivers_convert_to_delta')

# COMMAND ----------

df.write.format('parquet').save('/mnt/formula1-project/demo/drivers_convert_to_delta_new')

# COMMAND ----------

# DBTITLE 1,Converter um arquivo Parquet para Delta
# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1-project/demo/drivers_convert_to_delta_new`