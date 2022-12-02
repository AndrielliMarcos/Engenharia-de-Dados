# Databricks notebook source
# MAGIC %md
# MAGIC # Agregation
# MAGIC ###Objectives:
# MAGIC - Group data by specified columns
# MAGIC - Apply grouped data methods to agregate data
# MAGIC - Aply built-in functions to aggregate data

# COMMAND ----------

from pyspark.sql.functions import sum, avg, approx_count_distinct, cos, sqrt

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.5

# COMMAND ----------

# DBTITLE 1,Create dataframe
df = table("events")
display(df)

# COMMAND ----------

# MAGIC %md ### Grouping data
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# DBTITLE 1,Agroup for one column
df.groupBy("event_name")

# COMMAND ----------

# DBTITLE 1,Agroup for more columns
df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# MAGIC %md ### Grouped data methods
# MAGIC Various aggregation methods are available on the <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a> object.
# MAGIC 
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | agg | Compute aggregates by specifying a series of aggregate columns |
# MAGIC | avg | Compute the mean value for each numeric columns for each group |
# MAGIC | count | Count the number of rows for each group |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | min | Compute the min value for each numeric column for each group |
# MAGIC | pivot | Pivots a column of the current DataFrame and performs the specified aggregation |
# MAGIC | sum | Compute the sum for each numeric columns for each group |

# COMMAND ----------

# DBTITLE 1,groupBy() + count()
# agrupa pela coluna event_name e conta quantas linhas por agrupamento
event_count_df = df.groupBy("event_name").count()
display(event_count_df)

# COMMAND ----------

# DBTITLE 1,groupBy() + avg()
# agrupa pela coluna geo.state e faz a média dos valores agrupados
avg_state_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_df)

# COMMAND ----------

# DBTITLE 1,groupBy() + sum()
# agrupa pela coluna geo.state e geo.city e faz a soma dos valores agrupados
sum_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(sum_df)

# COMMAND ----------

# MAGIC %md ### Aggregate Functions
# MAGIC 
# MAGIC Here are some of the built-in functions available for aggregation.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | Returns the approximate number of distinct items in a group |
# MAGIC | avg | Returns the average of the values in a group |
# MAGIC | collect_list | Returns a list of objects with duplicates |
# MAGIC | corr | Returns the Pearson Correlation Coefficient for two columns |
# MAGIC | max | Compute the max value for each numeric columns for each group |
# MAGIC | mean | Compute the average value for each numeric columns for each group |
# MAGIC | stddev_samp | Returns the sample standard deviation of the expression in a group |
# MAGIC | sumDistinct | Returns the sum of distinct values in the expression |
# MAGIC | var_pop | Returns the population variance of the values in a group |
# MAGIC 
# MAGIC Use the grouped data method <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> to apply built-in aggregate functions
# MAGIC 
# MAGIC This allows you to apply other transformations on the resulting columns, such as <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>.

# COMMAND ----------

# DBTITLE 1,groupBy() + agg()
state_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_df)

#state_sum = df.groupBy("geo.state").sum("ecommerce.total_item_quantity")
#display(state_sum)

# nos 2 casos temos um agrupamento pela coluna "geo.state" e uma soma dos valores agrupados.
# o que difere os dataframes criados é que ao usar o agg() é possível renomear a coluna em tempo de execução

# COMMAND ----------

# DBTITLE 1,Multiple aggregate functions
# também é posível usar mais de uma função para fezer cálculo de agregação usando o agg()
state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"), # média por estado
                            approx_count_distinct("user_id").alias("distinct_users"))   # contagem de usuários distintos por estado
)
display(state_aggregates_df)

# COMMAND ----------

# MAGIC %md ### Math Functions
# MAGIC Here are some of the built-in functions for math operations.
# MAGIC 
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | ceil | Computes the ceiling of the given column. |
# MAGIC | cos | Computes the cosine of the given value. |
# MAGIC | log | Computes the natural logarithm of the given value. |
# MAGIC | round | Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode. |
# MAGIC | sqrt | Computes the square root of the specified float value. |

# COMMAND ----------

# DBTITLE 1,sqrt() e cos()
display(spark.range(10) # cria um dataframe com uma coluna 'id' com um range e de valores inteiros
        .withColumn("sqrt", sqrt("id")) # raiz quadrada do valor
        .withColumn("cos", cos("id")) # cosseno do valor 
)

# COMMAND ----------

# DBTITLE 1,Delete the tables and files associated with this lesson
DA.cleanup()
