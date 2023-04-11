# Databricks notebook source
# MAGIC %md
# MAGIC #Funções Python Definidas pelo Usuário
# MAGIC ##### Methods
# MAGIC - <a href="https://docs.databricks.com/spark/latest/spark-sql/udf-python.html" target="_blank">UDF Registration (**`spark.udf`**)</a>: **`register`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html?highlight=udf#pyspark.sql.functions.udf" target="_blank">Built-In Functions</a>: **`udf`**
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html" target="_blank">Python UDF Decorator</a>: **`@udf`**
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html" target="_blank">Pandas UDF Decorator</a>: **`@pandas_udf`**

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-02.7B

# COMMAND ----------

# criar um dataframe
sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# definir uma função para pegar a primeira letra da string email
def first_letter_function(email):
    return email[0]

first_letter_function("annagray@kaufman.com")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criar e Aplicar uma UDF
# MAGIC Registrar uma function como uma UDF. Isso serializa a função e a envia aos executores para poder transformar os registros do Dataframe.

# COMMAND ----------

first_letter_udf = udf(first_letter_function)

# COMMAND ----------

# aplicar a UDF na coluna email

from pyspark.sql.functions import col

display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Registrar uma UDF para usar em SQL
# MAGIC Registre a UDF usando **`spark.udf.register`** para também disponibilizá-la para uso no namespace SQL.

# COMMAND ----------

sales_df.createOrReplaceTempView("sales")

first_letter_udf = spark.udf.register("sql_udf", first_letter_function)

# COMMAND ----------

# You can still apply the UDF from Python
display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can now also apply the UDF from SQL
# MAGIC SELECT sql_udf(email) AS first_letter FROM sales

# COMMAND ----------

# MAGIC %md
# MAGIC ###Use Python Decorator Syntax
# MAGIC Alternativamente, você pode defibir e registrar uma UDF usando <a href="https://realpython.com/primer-on-python-decorators/" target="_blank">Python decorator syntax</a>. O parâmetro **`@udf`** é o tipo de dados da coluna que a função retorna.
# MAGIC 
# MAGIC Você não será mais capaz de chamar a função Python local (i.e., **`first_letter_udf("annagray@kaufman.com")`**).

# COMMAND ----------

# Our input/output is a string
@udf("string")
def first_letter_udf(email: str) -> str:
    return email[0]

# COMMAND ----------

from pyspark.sql.functions import col

sales_df = spark.table("sales")
display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Usando Pandas UDF

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")

# COMMAND ----------

display(sales_df.select(vectorized_udf(col("email"))))

# COMMAND ----------

# registrar Pandas UDF para o SQL namespace
spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

DA.cleanup()
