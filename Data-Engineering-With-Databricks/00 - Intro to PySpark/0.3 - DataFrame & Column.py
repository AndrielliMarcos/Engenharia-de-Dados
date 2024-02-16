# Databricks notebook source
# MAGIC %md
# MAGIC ## DataFrame & Column
# MAGIC 
# MAGIC ### Objectives:
# MAGIC This notebook has objectives:
# MAGIC - Construct columns
# MAGIC - Subset columns
# MAGIC - Add or replace columns
# MAGIC - Subset rows
# MAGIC - Sort rows

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.3

# COMMAND ----------

# DBTITLE 1,Create DataFrame
events_df = table("events")
# display(events_df)

# COMMAND ----------

# DBTITLE 1,Column Expressions
from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# DBTITLE 1,Column Operators and Methods
# MAGIC %md 
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | Math and comparison operators |
# MAGIC | ==, != | Equality and inequality tests (Scala operators are **`===`** and **`=!=`**) |
# MAGIC | alias | Gives the column an alias |
# MAGIC | cast, astype | Casts the column to a different data type |
# MAGIC | isNull, isNotNull, isNan | Is null, is not null, is NaN |
# MAGIC | asc, desc | Returns a sort expression based on ascending/descending order of the column |

# COMMAND ----------

# DBTITLE 1,Create complex expressions with esisting columns, operators and methods
# examples
col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
( col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# DBTITLE 1,Example of using column expressions
rev_df = (events_df
          .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull()) #filtrar pela coluna "ecommerce.purchase_revenue_in_usd" que não tenha valor nulo
          .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")) # cria nova coluna
          .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))# nova coluna  
          .sort(col("avg_purchase_revenue").desc()) # ordenar
         )

display(rev_df)

# COMMAND ----------

# DBTITLE 1,DataFrame Transformation Methods
# MAGIC %md
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`limit`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |

# COMMAND ----------

# DBTITLE 1,Select()
devices_df = events_df.select("user_id", "device")
display(devices_df)

# COMMAND ----------

# DBTITLE 1,Select()
from pyspark.sql.functions import col

locations_df = events_df.select(
    "user_id",
    col("geo.city").alias("city"),
    col("geo.state").alias("state")
)

display(locations_df)
# Note que as colunas que estavam dentro do array são extraídas e renomeadas

# COMMAND ----------

# DBTITLE 1,SelectExpre()
apple_df = events_df.selectExpr("user_id", "device", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)
# note que a coluna criada faz uma verificação se o conteúdo da coluna 'devise' está ou não na lista setada ('macOS', 'iOS')

# COMMAND ----------

# DBTITLE 1,Drop()
anonymous_df = events_df.drop("user_id", "geo", "device")
display(anonymous_df)

# COMMAND ----------

# DBTITLE 1,Drop()
no_sales_df = events_df.drop(col("ecommerce"))
display(no_sales_df)

# COMMAND ----------

# DBTITLE 1,WithColumn()
from pyspark.sql.functions import col

# withColumn() retorna um novo dataframe adicionando uma coluna ou substituindo uma coluna existente que tenha o mesmo nome.
mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobile_df)

#Note neste exemplo que foi criada uma nova coluna ("mobile") com o resultado da verificação de uma coluna já esxistente ("device")

# COMMAND ----------

# DBTITLE 1,WithColumn()
purchase_df = events_df.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchase_df.printSchema()

# criação de uma nova coluna ("purchase_quantity") com os valores de uma coluna existente mas que está dentro de um array ("ecommerce.total_item_quantity")

# COMMAND ----------

# DBTITLE 1,WithColumnRenamed()
# retorna um novo dataframe com uma coluna renomeada.
# neste caso a coluna geo passa a ser chamada location
location_df = events_df.withColumnRenamed("geo", "location") 
display(location_df)

# COMMAND ----------

# DBTITLE 1,Filter() - alias: where()
# faz um filtro baseado em uma condição
filter_df = events_df.filter("ecommerce.total_item_quantity > 0") # retorna onde a coluna "ecommerce.total_item_quantity > 0" é maior que 0
display(filter_df)

# COMMAND ----------

# DBTITLE 1,Filter() - alias: where()
revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull()) #retorna onte a coluna 'ecommerce.purchase_revenue_in_usd' não é nula
display(revenue_df)

# COMMAND ----------

# DBTITLE 1,Filter() - alias: where()
android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(android_df)

# COMMAND ----------

# DBTITLE 1,dropDuplicates() - alias: distinct()
# retorna um novo dataframe com as linhas duplicadas remove
display(events_df.distinct())

# COMMAND ----------

# DBTITLE 1,dropDuplicates() - alias: distinct()
events_df.count()

# COMMAND ----------

distinct_users_df = events_df.dropDuplicates(['user_id'])
display(distinct_users_df)

# COMMAND ----------

distinct_users_df.count()

# COMMAND ----------

# DBTITLE 1,limit()
# retorna um novo dataframe com o número de linhas definido
limit_df = events_df.limit(50)
display(limit_df)

# COMMAND ----------

# DBTITLE 1,sort() - alias: orderBy()
# retorna um novo dataframe ordenado pelas colunas ou expressão
increase_timestamp_df = events_df.sort("event_timestamp")
display(increase_timestamp_df)

# COMMAND ----------

# DBTITLE 1,sort() - alias: orderBy()
decrease_timestamp_df = events_df.sort(col("event_timestamp").desc())
display(decrease_timestamp_df)

# COMMAND ----------

# DBTITLE 1,sort() - alias: orderBy()
increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increase_sessions_df)

# COMMAND ----------

# DBTITLE 1,sort() - alias: orderBy()
decrease_sessions_df = events_df.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decrease_sessions_df)

# COMMAND ----------

# DBTITLE 1,Delete the tables and files associated with this lesson
DA.cleanup()
