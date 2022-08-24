# Databricks notebook source
# MAGIC %md
# MAGIC ### Este notebook tem o intuito de fazer um webscraping no site da Blue Store e verificar se os vouchers disponibilizados estão disponíveis para compra ou esgotados.
# MAGIC 
# MAGIC ### O retorno será o nome do voucher e o status.

# COMMAND ----------

# instalar a biblioteca beautifulsoup
%pip install beautifulsoup4

# COMMAND ----------

#import das bibliotecas
import requests
from bs4 import BeautifulSoup

# COMMAND ----------

# url origem dos dados
url_base = "https://store.blueshift.com.br/vouchers"

page = requests.get(url_base)
page # se o retorno for 200, a página está liberada para fazer o webscraping

# COMMAND ----------

soup = BeautifulSoup(page.text, 'html.parser')
soup # o retorno é o html do url acima

# COMMAND ----------

part1 = soup.find("div", {"id":"content"}) # ("tag", {"atributo":"valor do atributo"})
# part1

# COMMAND ----------

part2 = part1.findAll("div", {"class":"product-thumb"})
# part2

# COMMAND ----------

produtos = []
link = []
status = []

# COMMAND ----------

# part2
# part2[0].a.img['alt']
# part2[0].a['href']
# part2[0].find('div', {'class':'button-group'}).span.text

# COMMAND ----------

for i in part2: # para cada item da part2    
    produtos.append(i.a.img['alt'])
    link.append(i.a['href'])
    status.append(i.find('div', {'class':'button-group'}).span.text) 

# COMMAND ----------

# zipar as listas
zipalista = zip(produtos, link, status)

# COMMAND ----------

zipalista

# COMMAND ----------

# montar o schema
from pyspark.sql.types import *
from pyspark.sql.functions import current_date

schema = StructType([
    StructField("produto", StringType(), True),
    StructField("link", StringType(), True),
    StructField("status", StringType(), True)
])

# montar dataframe
df = spark.createDataFrame(zipalista, schema).withColumn("DATA", current_date())
df.display()
