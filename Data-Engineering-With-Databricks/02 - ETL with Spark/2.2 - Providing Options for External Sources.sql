-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Fornecendo opções para fontes externas
-- MAGIC Embora consultar arquivos diretamente funcione bem para formatos autodescritivos, muitas fontes de dados exigem configurações adicionais ou declaração de schema para ingerir registros corretamente.
-- MAGIC
-- MAGIC Nesta lição, nós iremos criar tabelas usando fontes de dados externas. Embora estas tabelas ainda não sejam armazenadas no formato Delta Lake(e, portanto, não sejam otimizadas para o Lakehouse), esta técnica ajuda a facilitar a extração de dados de diversos sistemas externos.
-- MAGIC
-- MAGIC ###Objetivos:
-- MAGIC - Usar Spark SQL para configurar opções para extrair dados de fontes externas
-- MAGIC - Criar tabelas de fontes de dados externos para vários formatos de arquivo
-- MAGIC - Descrever o comportamento padrão ao consultar tabelas definidas em fontes externas
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Quando as consultas diretas não funcionam
-- MAGIC Embora views podem ser usadas para persistir consultas diretas em arquivos entre as sessões, essa abordagem tem utilidade limitada.
-- MAGIC
-- MAGIC Arquivos CSV são um dos mais comuns formatos de arquivos, mas uma consulta direta neste arquivo raramente retorna o resultado desejado.

-- COMMAND ----------

SELECT * FROM csv.`${DA.paths.sales_csv}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Nós podemos vê pelo exposto que:
-- MAGIC 1. A linha do cabeçalho está sendo extraída como uma linha da tabela
-- MAGIC 1. Todas as colunas estão sendo carregadas como uma única coluna
-- MAGIC 1. O arquivo está usando pipe(**`|`**) como delimitador
-- MAGIC 1. A colula final parece conter dados aninhados que estão sendo truncados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Registrando tabelas em dados externos com opções de leitura
-- MAGIC Embora o Spark extraia algumas fontes de dados autodescritivas com eficiência usando as configurações padrão, muitos formatos irão exigir declaração do schema ou outras opções.
-- MAGIC
-- MAGIC Embora existam muitas <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">configurações adicionais</a>, você pode definir enquando cria as tabelas em fontes externas. A sintaxe a seguir mostra os requisitos essenciais para extrair dados da maioria dos formatos.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC Observe que as opções são passadas com chaves como textos sem aspas e valores entre aspas. Spark suporta muitas <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">fontes de dados</a> com opções personalizadas e sistemas adicionais podem ter suporte não oficial por meio de <a href="https://docs.databricks.com/libraries/index.html" target="_blank">bibliotecas externas</a>
-- MAGIC
-- MAGIC **Observação:** Dependendo das configurações do seu workspace, você pode precisar da assistencia do administrador para carregar bibliotecas e configurar os requisitos de segurança para algumas fontes de dados.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A célula abaixo demonstra o uso do Spark SQL DDL para criar uma tabela de uma fonte CSV externa, especificando:
-- MAGIC 1. O nome e o tipo da coluna
-- MAGIC 1. O formato do arquivo
-- MAGIC 1. O delimitador usado para separar os campos
-- MAGIC 1. A presença de um cabeçalho
-- MAGIC 1. O caminho para onde este dado será armazenado

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${DA.paths.sales_csv}"

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # spark.sql(f"""
-- MAGIC # CREATE TABLE IF NOT EXISTS sales_csv
-- MAGIC #   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
-- MAGIC # USING CSV
-- MAGIC # OPTIONS (
-- MAGIC #   header = "true",
-- MAGIC #   delimiter = "|"
-- MAGIC # )
-- MAGIC # LOCATION "{DA.paths.sales_csv}"
-- MAGIC # """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Todos os metadados e options passadas durante a declaração da tabela serão persistidos no metastore, garantindo que os dados no local irão sempre ser lidos com estas options.
-- MAGIC
-- MAGIC **Observação:** quando trabalhamos com CSVs como fonte de dados, é importante garantir que a ordem das colunas não mudem se arquivos de dados adicionais forem adicionados ao diretório de origem. O Spark carregará colunas e aplicará nomes de colunas e tipos de dados especificada durante a declaração da tabela.
-- MAGIC
-- MAGIC Executando **`DESCRIBE EXTENDED`** na tabela, irá mostrar todos os metadatas associados com a definição da tabela.

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Limites de tabelas com Fontes de Dados Externas
-- MAGIC Sempre que estamos definindo tabelas ou queries em fontes de dados externas, não podemos esperar as garantias de desempenho associadas ao Delta Lake e Lakehouse.
-- MAGIC
-- MAGIC Por exemplo: enquanto as tabelas do Delta Lake irão garantir que você sempre consulte a versão mais recente dos dados de origem, as tabelas registradas em outras fontes de dados podem representar versões mais antigas em cache.
-- MAGIC
-- MAGIC A célula abaixo executa uma lógica que nós podemos pensar como apenas representando um sistema externo atualizando diretamente os arquivos subjacentes à nossa tabela.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC       .option("header", "true")
-- MAGIC       .option("delimiter", "|")
-- MAGIC       .csv(DA.paths.sales_csv)
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(DA.paths.sales_csv))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Se observarmos a contagem atual de registros em nossa tabela, o número que vemos não refletirá essas novas linhas inseridas.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Os dados são armazenado em cache para que o Spark execute de forma mais rápida as consultas. E nossa fonte de dados externa não está configurada para informar ao Spark que ele deve atualizar esses dados.
-- MAGIC
-- MAGIC Podemos atualizar manualmente o cache de nossos dados executando o comando **`REFRESH TABLE`**

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Atualizar nossa tabela invalidará nosso cache, o que significa que precisaremos verificar novamente nossa fonte de dados original e puxar todos os dados de volta para memória.
-- MAGIC
-- MAGIC Para conjunto de dados grandes, essa operação pode levar alguns minutos.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Extraindo dados dos Databases SQL
-- MAGIC
-- MAGIC Databases SQL são uma fonte de dados extremamente comum, e o Databricks tem um driver JDBC padrão para conectar com muitos tipos de SQL.
-- MAGIC
-- MAGIC A sintaxe geral para criar esta conexão é: 
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC   
-- MAGIC No código abaixo, nós vamos conectar com o SQLite.
-- MAGIC   
-- MAGIC **Observação:** SQLite usa um arquivo local para armazenar um database e não requer uma posta, username ou senha.
-- MAGIC   
-- MAGIC **Atenção:** A configuração de backend do servidor JDBC assume que você está executando este notebook em um cluster de nó único (cluster single node)
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
  dbtable = "users"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora nós podemos consultar esta tabela como se fosse definida localmente.

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Embora a tabela esteja listada como **MANAGED**, listar o conteúdo do local especificado confirma que nenhum dado está sendo mantido localmente.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC location = spark.sql("DESCRIBE EXTENDED users_jdbc").filter(F.col("col_name") == "Location").first()["data_type"]
-- MAGIC print(location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(location)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que alguns sistemas SQL, como data warehouses, terão drivers personalizados. O Spark irá interagir com vários bancos de dados externos de maneira diferente, mas as duas abordagens básicas podem ser resumidas como:
-- MAGIC 1. Movendo toda tabela de origem para Databricks e, em seguida, executando a lógica no cluster atualmente ativo
-- MAGIC 1. Levando a consulta para o banco de dados SQL externo e transferindo apenas os resultados de volta para o Databricks
-- MAGIC
-- MAGIC Em ambos os casos, trabalhar com conjuntos de dados muito grandes em bancos de dados SQL externos pode gerar sobrecarga significativa devido a:
-- MAGIC 1. Latência de transferência de rede associada à movimentação de todos os dados pela Internet pública
-- MAGIC 1. Execução da lógica de consulta em sistemas de origem não otimizados para consultas de big data.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
