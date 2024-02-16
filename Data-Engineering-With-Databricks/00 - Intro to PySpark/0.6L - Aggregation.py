# Databricks notebook source
# MAGIC %md
# MAGIC ##Agregation Lab
# MAGIC The objective this notebook is to practice the topics of the notebook 0.5

# COMMAND ----------

from pyspark.sql.functions import sum, avg, approx_count_distinct, cos, sqrt, round, col

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.6L

# COMMAND ----------

# DBTITLE 1,Criação do DataFrame
# Purchase events logged on the BedBricks website
df = (spark.table("events")
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

# display(df)

# COMMAND ----------

# DBTITLE 1,Agrupar, calcular a soma e a média
traffic_df = df.groupBy("traffic_source")\
                .agg(sum("revenue").alias("total_rev"),\
                    avg("revenue").alias("avg_rev"))
             
display(traffic_df)   

# COMMAND ----------

# DBTITLE 1,Check work
expected1 = [(620096.0, 1049.2318), (4026578.5, 986.1814), (1200591.0, 1067.192), (2322856.0, 1093.1087), (826921.0, 1086.6242), (404911.0, 1091.4043)]
test_df = traffic_df.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 1,Ordenar decrescente pela coluna "total_rev" e mostrar as 3 primeiras linhas
top_traffic_df = (traffic_df
                  .sort(col("total_rev").desc())
                  .limit(3)
)
display(top_traffic_df)

# COMMAND ----------

# DBTITLE 1,Ckeck work
expected2 = [(4026578.5, 986.1814), (2322856.0, 1093.1087), (1200591.0, 1067.192)]
test_df = top_traffic_df.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected2 == result2)
print("All test pass")

# COMMAND ----------

# DBTITLE 1,Modificar as colunas "total_rev" e "avg_rev" para terem 2 casas decimais
final_df = (top_traffic_df
            .withColumn("total_rev", (col("total_rev") * 100).cast("long"))
            .withColumn("total_rev", (col("total_rev") / 100))
            .withColumn("avg_rev", (col("avg_rev") * 100).cast("long"))
            .withColumn("avg_rev", (col("avg_rev") / 100))
)

display(final_df)

# COMMAND ----------

# DBTITLE 1,Check work
expected3 = [(4026578.5, 986.18), (2322856.0, 1093.1), (1200591.0, 1067.19)]
result3 = [(row.total_rev, row.avg_rev) for row in final_df.collect()]

assert(expected3 == result3)
print("All test pass")

# COMMAND ----------

# DBTITLE 1,round() - modificar as colunas "total_rev" e "avg_rev" para terem 2 casas decimais
bonus_df = (top_traffic_df
            .select("traffic_source",round("total_rev", 2).alias("total_rev"))
                    
)

display(bonus_df)

# COMMAND ----------

# DBTITLE 1,Check work
expected4 = [(4026578.5), (2322856.0), (1200591.0)]
result4 = [(row.total_rev) for row in bonus_df.collect()]

assert(expected4 == result4)
print("All test pass")

# COMMAND ----------

# DBTITLE 1,Delete the tables and files associated with this lesson

DA.cleanup()
