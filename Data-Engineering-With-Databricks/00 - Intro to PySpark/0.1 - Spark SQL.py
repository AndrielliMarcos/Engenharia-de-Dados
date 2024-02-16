# Databricks notebook source
# MAGIC %md
# MAGIC ## Spark SQL
# MAGIC ###Objectives:
# MAGIC This notebook has objectives:
# MAGIC - Run a SQL query;
# MAGIC - Create a DataFrame from a table;
# MAGIC - Write the same query using DataFrame transformations;
# MAGIC - Trigger computation with DataFrame actions;
# MAGIC - Convert between DataFrames and SQL

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-00.1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Multiple Interfaces
# MAGIC We can interact with Spark SQL in to two mays:
# MAGIC 1. Executing SQL query;
# MAGIC 2. Working with the DataFrame API

# COMMAND ----------

# DBTITLE 1,Method 1: Executing SQL queries
# MAGIC %sql
# MAGIC SELECT name, price
# MAGIC FROM products
# MAGIC WHERE price < 200
# MAGIC ORDER BY price

# COMMAND ----------

# DBTITLE 1,Method 2: Working with the DataFrame API
display(spark
        .table("products")
        .select("name", "price")
        .where("price < 200")
        .orderBy("price")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SparkSession
# MAGIC The **SparkSession** class is the single entry point to all functionally in Spark using the DataFrame API.
# MAGIC 
# MAGIC The Spark Session is create for you and stored in a variable called **spark**

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 1,Use the method table to create a DataFrame
products_df = table("products")

# COMMAND ----------

# DBTITLE 1,Using SparkSession to run SQL
result_df = sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

display(result_df)

# COMMAND ----------

# DBTITLE 1,Using methods in the DataFrame API
budget_df = (spark
             .table("products")
             .select("name", "price")
             .where("price < 200")
             .orderBy("price")
            )
display(budget_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema
# MAGIC The schema defines the column names and types of a dataframe.

# COMMAND ----------

budget_df.schema

# COMMAND ----------

budget_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Some Actions

# COMMAND ----------

# DBTITLE 1,Show()
(products_df
     .select("name", "price")
     .where("price < 200")
     .orderBy("price")
     .show()
)

# COMMAND ----------

# DBTITLE 1,Count()
budget_df.count()

# COMMAND ----------

# DBTITLE 1,Collect()
budget_df.collect()
# returns an array of all rows in a DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ###Convert between DataFrame and SQL

# COMMAND ----------

budget_df.createOrReplaceTempView("budget")
#create a temporary view basead on the DataFrame

# COMMAND ----------

display(sql("SELECT * FROM budget"))

# COMMAND ----------

# DBTITLE 1,Delete the tables and files associated with this lesson
DA.cleanup()
