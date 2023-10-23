-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Objetos de Dados no Lakehouse
-- MAGIC Existem 5 principais **objetos de dados** no Databricks Lakehouse:
-- MAGIC - Catalog
-- MAGIC - Schema (Database)
-- MAGIC - Table
-- MAGIC - View
-- MAGIC - Function

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Um **catálogo** é um conjunto de databases. Um **database**, ou **schema**, é um agrupamento de objetos em um catálogo. Databases contém **tabelas**, **views** e **functions**.
-- MAGIC
-- MAGIC Uma **tabela** é uma coleção de linhas e colunas armazenadas como arquivos de dados no armazenamento de objetos.
-- MAGIC
-- MAGIC Uma **view** é uma consulta salva, normalmente em uma ou mais tabelas ou fontes de dados. Uma função é uma lógica salva que retorna um valor escalar ou um conjunto de linhas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As tabelas são classificadas como **managed tables** e **external tables**.
-- MAGIC
-- MAGIC **Managed tables** são baseadas em arquivos que são armazenados no local de armazenamento gerenciado que é configurado dentro do metastore. Quando uma tabela gerenciada é apagada, isto irá deletar a tabela e seus dados subjacentes.
-- MAGIC
-- MAGIC **External tables** são tabelas cujos arquivos de dados são armazenados em um local de armazenamento em nuvem, fora do local de armazenamento gerenciado, então apenas os metadados da tabela são gerenciados. A principal diferença funcional torna-se visível quando excluímos uma external table. Os dados subjacentes são retirados enquanto apenas a própria tabela é eliminada.
-- MAGIC
-- MAGIC **Managed tables** e **External tables** são tratadas da mesma forma do ponto de vista do controle de acesso.
