# Databricks notebook source
# MAGIC %md
# MAGIC Manter a qualidade dos dados e a confiabilidade dos dados em escala é complexo e frágil. Nem todas as dependências são facilmente expressas de forma linear e as empresas têm dezenas de colaboradores gerenciando centenas ou milhares de notebooks, Job, tableas e conjuntos de dados que dependem uns dos outros.
# MAGIC
# MAGIC Delta Live Tables foi projetado para permitir que os usuários declarem facilmente pipelines em batch e/ou streaming usando Python ou SQL. DLT tem integrado controle de qualidade e monitoramento da qualidade dos dados.
# MAGIC
# MAGIC Delta Live Table fornece observalidade clara em operações de pipeline e tratamento automático de erros.

# COMMAND ----------

# MAGIC %md
# MAGIC #Live Tables
# MAGIC São views materializadas para o Lakehouse.
# MAGIC - São definidas a partir de uma query SQL
# MAGIC - São criadas e mantidas atualizadas pelo pipeline
# MAGIC
# MAGIC **`CREATE LIVE TABLE report`**<br/>
# MAGIC **`AS SELECT sum(profit)`**<br/>
# MAGIC **`FROM prod.sales`**<br/>
# MAGIC
# MAGIC As Live Tables fornecem ferramentas para :
# MAGIC - gerenciar dependências
# MAGIC - controle de qualidade
# MAGIC - automatizar operações
# MAGIC - simplificar a colaboração
# MAGIC - reduzir a latência

# COMMAND ----------

# MAGIC %md
# MAGIC #Streaming Live Table
# MAGIC É baseado na estrutura Spark Streaming
# MAGIC - ela garante exatamente uma vez o processamento das linhas de entrada
# MAGIC - a diferença aqui é a instrução SQL:
# MAGIC
# MAGIC **`CREATE STREAMING LIVE TABLE report`**<br/>
# MAGIC **`AS SELECT sum(profit)`**<br/>
# MAGIC **`FROM cloud_files(prod.sales)`**<br/>
# MAGIC
# MAGIC Streaming Live Tables permitem reduzir custos e latência, evitando o reprocessamento de dados antigos.
# MAGIC
# MAGIC ### Usando s finção SQL STREAM()
# MAGIC **`CREATE STREAMING LIVE TABLE mystream`**<br/>
# MAGIC **`AS SELECT *`**<br/>
# MAGIC **`FROM STREAM(my_table)`**<br/>
# MAGIC
# MAGIC - my_table também é uma tabela stream.
# MAGIC - STREAM(my_table) lê um fluxo (stream) de novos registros em vez de um instantâneo. Tabelas streaming devem ser uma tabela apenas anexada.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Como usar o DLT:
# MAGIC - Escreva o **`CREATE LIVE TABLE`**
# MAGIC   - as definições são escretas, mas não são executadas no notebook
# MAGIC - Defina um pieline
# MAGIC   - selecione um ou mais netebooks e a configuração
# MAGIC - Clique em Start
# MAGIC   - DLT irá criar e atualizar todas as tabelas no pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dependência entre tabelas
# MAGIC Pode ser criado **live dependências** usando o **LIVE schema**. Exemplo:
# MAGIC
# MAGIC **`CREATE LIVE TABLE events`**<br/>
# MAGIC **`AS SELECT ...`**<br/>
# MAGIC **`FROM prod.raw_data`**<br/>
# MAGIC
# MAGIC As dependências LIVE do mesmo pipeline são lidas no **schema LIVE**. O DLT detecta dependências LIVE e executa todas as iperações na ordem correta. DLT lida com o paralelismo e captura a linhagem de dados.
# MAGIC
# MAGIC **`CREATE LIVE TABLE report`**<br/>
# MAGIC **`AS SELECT ...`**<br/>
# MAGIC **`FROM LIVE.events`**<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Garantia de dados corretos
# MAGIC Podemos garantir a exatidão usando **expectations**.
# MAGIC - Expectations são testes que garantem qualidade dos dados em produção.
# MAGIC
# MAGIC Exemplo em SQL:
# MAGIC
# MAGIC **`CONSTRAINT valid_timestamp`**<br/>
# MAGIC **`EXPECT(timestamp > '2012-01-01')`**<br/>
# MAGIC **`ON VIOLATION DROP`**<br/>
# MAGIC
# MAGIC Exemplo em Python:
# MAGIC
# MAGIC **`@dlt.expect_or_drop(`**<br/>
# MAGIC **`"valid_timestamp",`**<br/>
# MAGIC **`col("timestamp") > "2012-01-01")`**<br/>
# MAGIC
# MAGIC Para este exemplo, esperamos que um registro de data seja maior que '2012-01-01', e em caso de violação, os registros serão descartados..
# MAGIC
# MAGIC **Expectations são expressões verdadeiras ou falsas que são usadas para validar cada linha durante o processamento.**

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parâmetros
# MAGIC A configuração dos parâmetros é similar no Job Databricks, adicionando chave/valor no campo **configurations**.
# MAGIC Esses valores podem ser acessados usando **spark.conf.get()**.
