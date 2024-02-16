# Databricks notebook source
# MAGIC %md
# MAGIC ## Spark SQL Lab
# MAGIC The objective this notebook is to practice the topics of the notebook **0.1**.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.2L

# COMMAND ----------

# DBTITLE 1,Create a DataFrame from the events table
# MAGIC %python
# MAGIC events_df = table("events")

# COMMAND ----------

display(events_df)

# COMMAND ----------

# DBTITLE 1,Display DataFrame and inspect schema
events_df.schema
events_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Filter for rows where 'device' is 'macOS' and sort by 'event_timestamp'
from pyspark.sql.functions import col

# mac_df = events_df.filter(events_df.device == 'macOS')
mac_df = events_df.filter(col('device') == 'macOS').sort('event_timestamp')

# COMMAND ----------

display(mac_df)

# COMMAND ----------

# DBTITLE 1,Count results and take first 5 rows
num_rows = mac_df.count()
rows = mac_df.show(5)

# COMMAND ----------

# DBTITLE 1,Create the same Dataframe using SQL query
mac_sql_df = sql("""
SELECT * FROM events
WHERE device == 'macOS'
ORDER BY event_timestamp
""")

display(mac_sql_df)

# COMMAND ----------

# DBTITLE 1,Check work
verify_rows = mac_sql_df.take(5)
assert (mac_sql_df.select("device").distinct().count() == 1 and len(verify_rows) == 5 and verify_rows[0]['device'] == "macOS"), "Incorrect filter condition"
assert (verify_rows[4]['event_timestamp'] == 1592540419446946), "Incorrect sorting"
del verify_rows
print("All test pass")

# COMMAND ----------

# DBTITLE 1,Delete the tables and files associated with this lesson
DA.cleanup()
