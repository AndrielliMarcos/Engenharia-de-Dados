-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Tranformando Tipos Complexos
-- MAGIC Consultando dados tabulares armazenados no data lakehouse com Spark SQL é fácil, eficiente e rápido.
-- MAGIC 
-- MAGIC Isso fica mais complicado à medida que a estrutura de dados se torna menos regular, quando muitas tabelas precisam ser usadas em uma única consulta ou quando a forma dos dados precisa ser alterada drasticamente. Este notebook introduz um número de funções presentes no Spark SQL para ajudar engenheiros a concluir até mesmo as transformações mais complicadas.
-- MAGIC 
-- MAGIC ### Objetivos:
-- MAGIC - Usar sintaxe **`.`** e **`:`** para consultar dados aninhados
-- MAGIC - Analisar strings JSON em estruturas
-- MAGIC - Desempacotar arrays e structs
-- MAGIC - Combinar conjunto de dados usando joins
-- MAGIC - Remodelar dados usando tabelas dinâmicas

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Visão geral dos dados
-- MAGIC A tabela **`events_raw`** foi registrada em relação aos dados que representam uma carga Kafka. Na maioria dos casos, dados kafka serão valores JSON codificados em binário.
-- MAGIC 
-- MAGIC Vamos converter a **`key`** e **`value`** como strings para visualizar esses valores em um formato legível por humanos.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS 
SELECT string(key), string(value) FROM events_raw;

SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC events_stringsDF = (spark
-- MAGIC     .table("events_raw")
-- MAGIC     .select(col("key").cast("string"), 
-- MAGIC             col("value").cast("string"))
-- MAGIC     )
-- MAGIC display(events_stringsDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Trabalhar com dados aninhados
-- MAGIC O código na célula abaixo consulta as strings convertidas para visualizar um exemplo de objeto JSON sem campos nulos.
-- MAGIC 
-- MAGIC **Observe:** Spark SQL tem funcionalidades integradas para interagir diretamente com dados aninhados armazenados como strings JSON ou tipos struct.
-- MAGIC - Use sintaxe **`:`** em consultas para acessar subcampos em strings JSON
-- MAGIC - Use sintaxe **`.`** em consultas para acessar subcampos em tipo struct

-- COMMAND ----------

SELECT * FROM events_strings WHERE value:event_name = "finalize" ORDER BY key LIMIT 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(events_stringsDF
-- MAGIC     .where("value:event_name = 'finalize'")
-- MAGIC     .orderBy("key")
-- MAGIC     .limit(1)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos usar a string JSON acima para obter o schema, em seguida analise toda a coluna JSON em tipos struct.  
-- MAGIC - **`schema_of_json()`** retorna o schema obtido de um exemplo JSON string
-- MAGIC - **`from_json()`** analisa uma coluna contendo uma string JSON em um tipo struct usando o esquema especificado.
-- MAGIC 
-- MAGIC Depois de desempacotar a string JSON para um tipo struct, vamos desempacotar e nivelar todos os campos struct em colunas.
-- MAGIC - **`*`: `col_name.*`** extrai os subcampos de **`col_name`** em sua próprias colunas.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT json.* FROM (
SELECT from_json(value, schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')) AS json 
FROM events_strings);

SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, schema_of_json
-- MAGIC 
-- MAGIC json_string = """
-- MAGIC {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}
-- MAGIC """
-- MAGIC parsed_eventsDF = (events_stringsDF
-- MAGIC     .select(from_json("value", schema_of_json(json_string)).alias("json"))
-- MAGIC     .select("json.*")
-- MAGIC )
-- MAGIC 
-- MAGIC display(parsed_eventsDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Manipulando Arrays
-- MAGIC Spark SQL tem um número de funções para manipular dados array, incluindo:
-- MAGIC - **`explode()`** separa os elementos de um array em várias linhas. Isto cria uma nova linha para cada elemento
-- MAGIC - **`size()`** fornece uma contagem para o número de elementos no array para cada linha.
-- MAGIC 
-- MAGIC O código a seguir, separa o campo **`items`** (um array de structs) em várias linhas e mostra eventos contendo arrays com 3 ou mais itens.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW exploded_events AS
SELECT *, explode(items) AS item
FROM parsed_events;

SELECT * FROM exploded_events WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, size
-- MAGIC 
-- MAGIC exploded_eventsDF = (parsed_eventsDF
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC 
-- MAGIC display(exploded_eventsDF.where(size("items") > 2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O código a seguir combina transformações de array para criar uma tabela que mostra a única coleção de ações e os itens no carrinho de um usuário.
-- MAGIC - **`collect_set()`** coleta valores únicos por campo, incluindo campos dentro de arrays
-- MAGIC - **`flatten()`** combina vários arrays dentro de um único array
-- MAGIC - **`array_distinct()`** remove elementos duplicados de um array

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM exploded_events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import array_distinct, collect_set, flatten
-- MAGIC 
-- MAGIC display(exploded_eventsDF
-- MAGIC     .groupby("user_id")
-- MAGIC     .agg(collect_set("event_name").alias("event_history"),
-- MAGIC             array_distinct(flatten(collect_set("items.item_id"))).alias("cart_history"))
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Join entre tabelas
-- MAGIC Spak SQL suporta operações padrão de **`JOIN`** (inner, outer, left, right, anti, cross, semi).
-- MAGIC Aqui, juntamos o conjunto de dados de eventos separados (exploded) com uma tabela de pesquisa para obter o nome do item impresso.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW item_purchases AS

SELECT * 
FROM (SELECT *, explode(items) AS item FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM item_purchases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC exploded_salesDF = (spark
-- MAGIC     .table("sales")
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC 
-- MAGIC itemsDF = spark.table("item_lookup")
-- MAGIC 
-- MAGIC item_purchasesDF = (exploded_salesDF
-- MAGIC     .join(itemsDF, exploded_salesDF.item.item_id == itemsDF.item_id)
-- MAGIC )
-- MAGIC 
-- MAGIC display(item_purchasesDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Tabelas Pivot
-- MAGIC Podemos usar **`PIVOT`** para visualizar dados de diferentes perspectivas rotacionando valores exclusivos em uma coluna dinâmica específica em várias colunas com base em uma função agregada.
-- MAGIC - A cláusula **`PIVOT`** segue o nome da tabela ou a subconsulta especificada na cláusula **`FROM`**, que é a entrada para a tabela pivot
-- MAGIC - Valores únicos na coluna pivot são agrupados e agregados usando a expressão agregada fornecida, criando uma coluna separada para cada valor único na tabela pivot resultante
-- MAGIC 
-- MAGIC O código da célula a seguir usa **`PIVOT`** para nivelar as informações de compra do item contidas em vários campos derivados do conjunto de dados **`sales`**. Esse formato de dados nivelado pode ser útil para dashboards, mas também para aplicar algoritmos de marchine learning para inferência ou previsão.

-- COMMAND ----------

SELECT *
FROM item_purchases
PIVOT (
  sum(item.quantity) FOR item_id IN (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K')
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC transactionsDF = (item_purchasesDF
-- MAGIC     .groupBy("order_id", 
-- MAGIC         "email",
-- MAGIC         "transaction_timestamp", 
-- MAGIC         "total_item_quantity", 
-- MAGIC         "purchase_revenue_in_usd", 
-- MAGIC         "unique_items",
-- MAGIC         "items",
-- MAGIC         "item",
-- MAGIC         "name",
-- MAGIC         "price")
-- MAGIC     .pivot("item_id")
-- MAGIC     .sum("item.quantity")
-- MAGIC )
-- MAGIC display(transactionsDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
