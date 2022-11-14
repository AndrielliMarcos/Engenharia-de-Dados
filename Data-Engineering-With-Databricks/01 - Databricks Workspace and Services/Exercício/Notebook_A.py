# Databricks notebook source
name = "Jonh"

# COMMAND ----------

print(f"Hello {name}")

# COMMAND ----------

# MAGIC %run ./Notebook_B

# COMMAND ----------

print (f"Welcome back {name} {full_name}")

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Create a Markdown Cell
# MAGIC 
# MAGIC Add a new cell below this one. Populate with some Markdown that includes at least the following elements:
# MAGIC * A header
# MAGIC * Bullet points
# MAGIC * A link (using your choice of HTML or Markdown conventions)

# COMMAND ----------

# MAGIC %md #This is a header
# MAGIC - bullet 1
# MAGIC - bullet 2
# MAGIC - [Example link](https://translate.google.com.br/?hl=pt-BR&tab=rT&sl=en&tl=pt&text=rathe&op=translate)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/nyctaxi-with-zipcodes/subsampled`

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(files)
