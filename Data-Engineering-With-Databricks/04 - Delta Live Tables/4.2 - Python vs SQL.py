# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables: Python vs SQL
# MAGIC Nesta lição iremos revisar as principai diferenças entre as implementações Python e SQL de Delta Live Tables.

# COMMAND ----------

# MAGIC %md # Python vs SQL
# MAGIC | Python | SQL | Notes |
# MAGIC |--------|--------|--------|
# MAGIC | Python API | Proprietary SQL API |  |
# MAGIC | no dlt module, no syntax check | has syntax checks| Em Python, se você executar uma célula de notebook DLT por conta própria, ela mostrará um erro, enquanto no SQL ela verificará se o comando é sintaticamente válido e informará. Em ambos os casos, células de notebook individuais são devem ser executadas para pipeline DLT |
# MAGIC | A Note on Imports | None | O módulo dlt deve ser explicitamente importado para suas bibliotecas de notebook Python. No SQL isso não acontece |
# MAGIC | Tables as DataFrames | Tables as Query Results | A API Python DataFrame permite várias transformações de um conjunto de dados, agrupando várias chamadas de API. Em comparação com o SQL, essas mesmas transformações devem ser salvas em tabelas temporárias à medida que são transformadas |
# MAGIC |@dlt.table()  | The Select Statement | No SQL, a lógica principal de sua consulta, contendo as transformações feitas em seus dados, está contida na instrução SELECT. Em Python, as transformações de dados são especificadas quando você configura options para @dlt.table()  |
# MAGIC | @dlt.table(comment = "Python comment",table_properties = {"quality": "silver"}) | COMMENT "SQL comment"       TBLPROPERTIES ("quality" = "silver") | É assim que você adiciona comentários e propriedades de tabela em Python vs SQL |
