# Databricks notebook source
# MAGIC %md
# MAGIC ## DataFrame & Column Lab
# MAGIC The objective this notebook is to practice the topics of the notebook 0.3

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.4L

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

events_df = table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 1,Add and extracting column
revenue_df = events_df.withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
display(revenue_df)

# COMMAND ----------

# DBTITLE 1,Check work
expected1 = [4351.5, 4044.0, 3985.0, 3946.5, 3885.0, 3590.0, 3490.0, 3451.5, 3406.5, 3385.0]
result1 = [row.revenue for row in revenue_df.sort(col("revenue").desc_nulls_last()).limit(10).collect()]
print(result1)
assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 1,Filter records where revenue is not null
purchases_df = revenue_df.filter(col("revenue").isNotNull())
display(purchases_df)

# COMMAND ----------

# DBTITLE 1,Check work
assert purchases_df.filter(col("revenue").isNull()).count() == 0, "Nulls in 'revenue' column"
print("All test pass")

# COMMAND ----------

# DBTITLE 1,Unique values - distinct()
distinct_df = purchases_df.distinct()
display(distinct_df)

# COMMAND ----------

# DBTITLE 1,Unique values - dropDuplicates()
dropDuplicate_df = purchases_df.dropDuplicates(['device'])
display(dropDuplicate_df)

# COMMAND ----------

# DBTITLE 1,Drop column
final_df = purchases_df.drop("event_name")
display(final_df)

# COMMAND ----------

# DBTITLE 1,Check work
expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(final_df.columns) == expected_columns)
print("All test pass")

# COMMAND ----------

# DBTITLE 1,All the steps
final_df = events_df\
            .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))\
            .filter(col("revenue").isNotNull())\
            .drop("event_name")
display(final_df)

# COMMAND ----------

# DBTITLE 1,Check work
assert(final_df.count() == 9056)
print("All test pass")

# COMMAND ----------

expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(final_df.columns) == expected_columns)
print("All test pass")

# COMMAND ----------

# DBTITLE 1,Delete the tables and files associated with this lesson
DA.cleanup()
