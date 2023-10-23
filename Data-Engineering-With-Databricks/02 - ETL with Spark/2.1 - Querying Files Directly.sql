-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Extraindo dados diretamente de arquivos com Spark SQL
-- MAGIC Neste notebook, você irá aprender a extrair dados diretamente dos arquivos usando Spark SQL no Databricks.
-- MAGIC
-- MAGIC Vários formatos de arquivos suportam esta opção, mas é mais útil para formatos de dados autodescritivos (como Parquet e JSON).
-- MAGIC
-- MAGIC ###Objetivos:
-- MAGIC - Usar Spark SQL para consultar arquivos de dados diretamente
-- MAGIC - Visualizações de camada (views) e CTEs (common table expression) para facilitar a referência de arquivos de dados
-- MAGIC - Aproveitar métodos **`text`** e **`binaryFile`** para revisar o conteúdo dos arquivos brutos
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Executar as configurações
-- MAGIC O script de configuração irá criar os dados e declarar os valores necessários para este notebook ser executado.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Visão geral dos dados
-- MAGIC Neste exemplo, nós iremos trabalhar com uma amostra de dados KAFKA brutos gravados como arquivos JSON.
-- MAGIC
-- MAGIC Cada arquivo contém todos os registros consumidos durante um intervalo de 5 segundos.
-- MAGIC | field | type | description |
-- MAGIC | --- | --- | --- |
-- MAGIC | key | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | timestamp | LONG | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que nosso diretório fonte contém muitos arquivos JSON.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Aqui, nós usaremos caminhos de arquivo relativos aos dados que foram gravados na raiz do DBFS.
-- MAGIC
-- MAGIC A maioria dos workflows irão requerer usuários para acessar os dados do local externo de armazenamento na nuvem.
-- MAGIC
-- MAGIC Na maioria das empresas, um administrador de workspace será responsável por configurar o acesso para esses locais de armazenamento.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Consultando um único arquivo
-- MAGIC Para consultar os dados contidos em um arquivo único, execute a query com o seguinte padrão:
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que a visualização retornou 321 linhas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Consultando um diretório de arquivos
-- MAGIC Assumindo que todos os arquivos no diretório tem o mesmo formato e schema, todos os arquivos podem ser consultados simultaneamente especificando o caminho do diretório em vez de um arquivo individual.
-- MAGIC

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar referências para um arquivo (view)
-- MAGIC Essa capacidade de consultar arquivos e diretórios diretamente significa que a lógica adicional do Spark pode ser encadeado para consultas em arquivos.
-- MAGIC
-- MAGIC Quando nós criamos uma view de uma consulta a uma path, podemos referenciar esta view em consultas posteriores.

-- COMMAND ----------

CREATE OR REPLACE VIEW event_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

SELECT * FROM event_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar Referência Temporária para os arquivos (Temp View)
-- MAGIC As Temp Views são semelhantes as consultas para um nome que seja mais fácil de referenciar em consultas posteriores.
-- MAGIC
-- MAGIC **As Temp Views existem somente para o SparkSession atual. No Databricks, isso significa que elas são isoladas para o notebook, job ou query DBSQL atuais.**

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

SELECT * FROM events_temp_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Aplicar CTEs para referência dentro de uma consulta
-- MAGIC As CTEs trazem uma referência de curta duração para os resultados de uma consulta.
-- MAGIC
-- MAGIC Uma CTE só consegue trazer um resultado estando tudo dentro de uma mesma célula (consulta sendo planejada e executada). Se tentar consultar uma CTE em outra célula, a consulta retornará um erro

-- COMMAND ----------

WITH cte_json
AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
-- SELECT * FROM cte_json
SELECT COUNT(*) FROM cte_json

-- COMMAND ----------

-- SELECT * FROM cte_json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Extrair arquivos de texto como string brutas
-- MAGIC Quando trabalhamos com arquivos de texto (JSON, CSV, TSV e TXT), podemos usar o formato **`text`** para carregar cada linha dos arquivos como uma linha com uma coluna de string chamada **`value`**. Isto pode ser útil quando as fontes de dados são propensas a corrupção e funções personalizadas de análise de texto serão usadas para extrair valores de campos de texto.

-- COMMAND ----------

SELECT * FROM text.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Extrair os bytes e os metadados brutos de um arquivo
-- MAGIC Alguns workflows podem exigir o trabalho com arquivos inteiros, como ao lidar com imagens ou dados não estruturados. Usando **`binaryFile`** para consultar um diretório, fornecerá metadados de arquivo juntamente com a representação binária do conteúdo do arquivo.
-- MAGIC
-- MAGIC Especificamente, os campos criados indicarão a **`path`**, **`modificationTime`**, **`length`**,  **`content`**.

-- COMMAND ----------

SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # delete as tabelas e arquivos associados a esta lição
-- MAGIC DA.cleanup()
