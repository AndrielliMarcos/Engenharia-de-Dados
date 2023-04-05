-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Dados de Limpeza
-- MAGIC À medida que nós inspecionamos e limpamos nossos dados, nós precisaremos construir várias expressões de coluna e consultas para expressar transformações a serem aplicadas em nosso conjunto de dados.
-- MAGIC 
-- MAGIC Expressões de coluna são construídas de colunas existentes, operadores e funções integradas existentes. Elas podem ser usadas em instruções **SELECT** para expressar transformações que criam novas colunas.
-- MAGIC 
-- MAGIC Muitos comandos de consulta SQL padrão (e.g. **`DISTINCT`**, **`WHERE`**, **`GROUP BY`**, etc.) estão disponíveis no Spark SQL para expressar transformações.
-- MAGIC 
-- MAGIC Neste notebook, iremos rever alguns conceitos que podem diferir de outros sistemas aos quais você está acostumado, além de destacar algumas funções úteis para operações comuns.
-- MAGIC 
-- MAGIC Daremos atenção especial aos comportamentos em torno de valores **NULL**, bem como à formatação de strings e campos de data e hora.
-- MAGIC 
-- MAGIC ###Objetivos:
-- MAGIC - Resumir conjuntos de dados e descrever comportamentos nulos
-- MAGIC - Recupere e remova valores duplicados
-- MAGIC - Valide conjunto de dados para contagens esperadas, valores ausentes e registros duplicados
-- MAGIC - Aplique transformações comuns para limpar e transformar os dados

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Visão Geral dos dados
-- MAGIC Trabalharemos com novos registros de usuários da tabela **`users_dirty`**, que tem o schema a seguir:
-- MAGIC | field | type | description |
-- MAGIC |---|---|---|
-- MAGIC | user_id | string | unique identifier |
-- MAGIC | user_first_touch_timestamp | long | time at which the user record was created in microseconds since epoch |
-- MAGIC | email | string | most recent email address provided by the user to complete an action |
-- MAGIC | updated | timestamp | time at which this record was last updated |
-- MAGIC 
-- MAGIC Vamos começar contando os valores de cada campo da nossa tabela.

-- COMMAND ----------

SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Inspecionando dados ausentes
-- MAGIC Baseado na contagem acima, parece que há pelo menos alguns valores nulos em todos os campos.
-- MAGIC 
-- MAGIC **Observação:** Valores nulos se comportam incorretamente em algumas funções matemáticas, incluindo **`count()`**.
-- MAGIC - **`count(col)`** pula valores **nulos** quando colunas ou expressões específicas
-- MAGIC - **`count(*)`** é um caso especial que conta o número total de linhas (incluindo linhas cujo valor é **null**)
-- MAGIC 
-- MAGIC Nós podemos contar os valores **null** em um campo filtrando os registros em que esse campo é nulo:
-- MAGIC **`count_if(col IS NULL)`** ou **`count(*)`** com um filtro **where** **`col IS NULL`**. 
-- MAGIC 
-- MAGIC Ambas as declarações acabixo contam corretamente os registros com os emails ausentes.

-- COMMAND ----------

SELECT count_if(email IS NULL) FROM users_dirty;
SELECT count(*) FROM users_dirty WHERE email IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("users_dirty")
-- MAGIC 
-- MAGIC usersDF.selectExpr("count_if(email IS NULL)")
-- MAGIC usersDF.where(col("email").isNull()).count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Linhas Duplicadas
-- MAGIC Podemos usar o **`DISTINCT *`** para remover registros duplicados verdadeiros em que linhas inteiras contêm os mesmos valores. 

-- COMMAND ----------

SELECT DISTINCT(*) FROM users_dirty

-- COMMAND ----------

-- MAGIC %python
-- MAGIC usersDF.distinct().count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Linhas duplicadas baseadas em colunas específicas
-- MAGIC O código abaixo usa **`GROUP BY`** para remover registros duplicados baseados nos valores das colunas **`user_id`** e **`user_first_touch_timestamp`**. (Lembre-se que estes dois campos são gerados quando um determinado usuário é encontrado pela primeira vez, formando assim tuplas únicas).
-- MAGIC 
-- MAGIC Aqui, estamos usando a função de agregação *`max`* como um truque para:
-- MAGIC - Manter valores das colunas **`email`** e **`updated`** no resultado no nosso group by
-- MAGIC - Capturar emails não nulos quando registros múltiplos estão presentes

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import max
-- MAGIC dedupedDF = (usersDF
-- MAGIC     .where(col("user_id").isNotNull())
-- MAGIC     .groupBy("user_id", "user_first_touch_timestamp")
-- MAGIC     .agg(max("email").alias("email"), 
-- MAGIC          max("updated").alias("updated"))
-- MAGIC     )
-- MAGIC 
-- MAGIC dedupedDF.count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos confirmar que temos a contagem esperada de registros restantes após a desduplicação com base nos valores distintos **`user_id`** e **`user_first_touch_timestamp`**

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (usersDF
-- MAGIC     .dropDuplicates(["user_id", "user_first_touch_timestamp"])
-- MAGIC     .filter(col("user_id").isNotNull())
-- MAGIC     .count())

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validar os conjuntos de dados
-- MAGIC Com base em nossa revisão manual acima, confirmamos visualmente que nossas contagens são as esperadas.
-- MAGIC 
-- MAGIC Podemos também executar validações programaticamente usando filtros simples e cláusulas **WHERE**.
-- MAGIC 
-- MAGIC Valide que o **`user_id`** para cada linha é único.

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC 
-- MAGIC display(dedupedDF
-- MAGIC     .groupby("user_id")
-- MAGIC     .agg(count("*").alias("row_count"))
-- MAGIC     .select((max("row_count") <= 1).alias("no_duplicate_ids")))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Confirme que cada email é associado com no máximo um **`user_id`**.

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(dedupedDF
-- MAGIC     .where(col("email").isNotNull())
-- MAGIC     .groupby("email")
-- MAGIC     .agg(count("user_id").alias("user_id_count"))
-- MAGIC     .select((max("user_id_count") <= 1).alias("at_most_one_id")))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Formato de Data e Regex
-- MAGIC Agora que os campos nulos e duplicados foram eliminados, podemos desejar extrair mais valor dos dados.
-- MAGIC 
-- MAGIC O código abaixo:
-- MAGIC - Dimensiona e converte corretamente o **`user_first_touch_timestamp`** para um **`timestamp`** válido
-- MAGIC - Extrai os dados do calendário e a hora do relógio para o **`timestamp`** em formato legível por humanos
-- MAGIC - Usa **`regexp_extract`** para extrair o comínio da coluna email usando regex

-- COMMAND ----------

SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import date_format, regexp_extract
-- MAGIC 
-- MAGIC display(dedupedDF
-- MAGIC     .withColumn("first_touch", (col("user_first_touch_timestamp") / 1e6).cast("timestamp"))
-- MAGIC     .withColumn("first_touch_date", date_format("first_touch", "MMM d, yyyy"))
-- MAGIC     .withColumn("first_touch_time", date_format("first_touch", "HH:mm:ss"))
-- MAGIC     .withColumn("email_domain", regexp_extract("email", "(?<=@).+", 0))
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
