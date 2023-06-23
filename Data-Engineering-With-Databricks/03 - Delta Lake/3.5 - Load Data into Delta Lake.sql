-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Carregando dados para o Delta Lake
-- MAGIC
-- MAGIC As tabelas Delta Lake fornecem atualizações compatíveis com ACID para tabelas apoiadas por arquivos de dados no armazenamento de objetos em nuvem.
-- MAGIC
-- MAGIC Neste notebook, iremos explorar a sintaxe SQL para processar atualizações com Delta Lake. Embora muitas operações sejam padrão SQL, existem pequenas variações para acomodar a execução do Spark e do Delta Lake.
-- MAGIC
-- MAGIC ### Objetivos:
-- MAGIC - Sobrescrever tabelas de dados usando **`INSERT OVERWRITE`**
-- MAGIC - Anexar dados em uma tabela usando **`INSERT INTO`**
-- MAGIC - Anexar, atualizar e deletar de uma tabela usando **`MERGE INTO`**
-- MAGIC - Ingerir dados de forma incremental em tabelas usando **`COPY INTO`**

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Sobrescrever (overwrite)
-- MAGIC Podemos usar o *overwrite* para substituir atomicamente todos os dados em uma tabela. Existem vários benefícios em substituir tabelas em vez de excluir e recriar:
-- MAGIC - Sobrescrever uma tabela é muito mais rápido porque não precisa listar o diretório recursivamente ou excluir nenhum arquivo
-- MAGIC - A versão antiga da tabela ainda existe. E pode facilmente recuperar os dados antigos usando Time Travel.
-- MAGIC - É uma operação atômica. Consultas concorrentes ainda podem ler as tabelas enquanto você está deletando a tabela.
-- MAGIC - Devido as garantias de transação ACID, se a substituição da tabela falhar, a tabela estará em seu estado anterior.
-- MAGIC
-- MAGIC O Spark SQL fornece dois métodos fáceis para realizar um overwrite completo.
-- MAGIC
-- MAGIC A instrução **`CREATE OR REPLACE TABLE`** (CRAS) substitui totalmente o conteúdo de uma tabela cada vez que são executadas.

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/events-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A revisão do histórico da tabela mostra que uma versão anterior desta tabela foi substituída.

-- COMMAND ----------

DESCRIBE HISTORY events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`INSERT OVERWRITE`** fornece um resultado quase idêntico ao anterior: os dados na tabela destino serão substituídos pelos dados da consulta.
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`:**
-- MAGIC - Podem sobrescrever somente uma tabela existente, não cria uma nova como a instrução CRAS
-- MAGIC - Podem sobrescrever somente com novos registros que correspondam ao esquema da tabela atual - e, portanto, pode ser uma técnica mais segura para sobrescrever uma tabela existente sem interromper os consumidores downstream
-- MAGIC - Podem sobrescrever partições individuais

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que são exibidas diferentes métricas de uma instrução CRAS. O histórico da tabela também registra a operação de forma diferente.

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A principal diferença aqui tem haver com a forma como o Delta Lake aplica o esquema na gravação.
-- MAGIC
-- MAGIC Enquanto que a instrução CRAS irá nos permitir redefinir completamente o conteúdo da nossa tabela destino, **`INSERT OVERWRITE`** irá falhar se tentarmos mudar nosso schema (a menos que forneçamos configurações opcionais).
-- MAGIC
-- MAGIC Descomente e execute a célula abaixo para gerar uma mensagem de erro esperada.

-- COMMAND ----------

-- INSERT OVERWRITE sales
-- SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Acrescentar(append) Linhas
-- MAGIC Nós podemos usar **`INSERT INTO`** para acrescentar atomicamente novas linhas em uma tabela Delta existente. Isso permite atualizações incrementais em uma tabela existente, o que é muito mais eficiente que sobrescrever todas as vezes.
-- MAGIC
-- MAGIC Acrescentar novos registros de venda na tabela **`sales`** usando **`INSERT INTO`**.

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-30m`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que **`INSERT INTO`** não tem nenhuma garantia integrada para impedir a inserção dos mesmos registros várias vezes. Reexecutando a célula acima gravaria os mesmos registros na tabela de destino, resultando registros duplicados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Mesclar (MERGE) atualizações
-- MAGIC Você pode fazer um *upsert* (atualizar e inserir) de dados de uma tabela origem, view ou DataFrame em uma tabela Delta destino usando a operação **`MERGE SQL`**. O Delta Lake suporta *insert*, *update* e *delete* no **`MERGE`**, e suporta sintaxe estendida além dos padrões SQL para facilitar casos de uso avançados.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC Iremos usar a operação **`MERGE`** para atualizar dados históricos de usuários com e-mails atualizados e novos usuários.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-30m`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Os principais benefícios do **`MERGE`**:
-- MAGIC - updates, inserts e deletes são concluídos como uma única transação
-- MAGIC - vários condicionais podem ser adicionados além dos campos correspondentes
-- MAGIC - fornece amplas opções para implementar uma lógica personalizada
-- MAGIC
-- MAGIC A seguir, iremos atualizar apenas os registros se a linha atual tiver um email **`NULL`** e a nova linha não.
-- MAGIC
-- MAGIC Todos os registros não correspondentes do novo lote serão inseridos.

-- COMMAND ----------

MERGE INTO users a -- tabela destino
USING users_update b -- tabela origem
ON a.user_id = b.user_id -- onde os ids são iguais
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN -- quando os ids forem iguais, e email de 'a' é null e email de 'b' não é null, 
  UPDATE SET email = b.email, updated = b.updated -- então atualiza com o email de 'b'
WHEN NOT MATCHED THEN INSERT * --quando os ids não forem iguai, insere o novo id


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que especificamos explicitamente o comportamento dessa função para as condições **`MATCHED`** e **`NOT MATCHED`**. O exemplo demostrado aqui é apenas um exemplo de lógica que pode ser aplicada, ao invés de indicativo de todo comportamento **`MERGE`**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Mesclagem (MERGE) somente de inserção (insert) para desduplicação
-- MAGIC Um caso de uso comum de ETL é coletar logs ou outros conjuntos de dados de cada anexação em uma tabela Delta por meio de uma série de operações de anexação.
-- MAGIC
-- MAGIC Muitos sistemas de origem podem gerar registros duplicados. Com a mesclagem (MERGE), você pode evitar inserir os registros duplicados executando uma mesclagem (MERGE) somente na inserção.
-- MAGIC
-- MAGIC Esse comando otimizado usa a mesma sintaxe **`MERGE`** mas fornece somente uma cláusula **`WHEN NOT MATCHED`**.
-- MAGIC
-- MAGIC A seguir, usamos isto para confirmar que os registros com o mesmo **`user_id`** e **`event_timestamp`** ainda não estão na tabela **`events`**.

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Carregar incrementalmente
-- MAGIC **`COPY INTO`** fornece aos engenheiros de SQL uma opção para ingerir dados de forma incremental de sistemas externos.
-- MAGIC
-- MAGIC Observe que esta operação tem algumas expectativas:
-- MAGIC - o schema de dados deve ser consistente
-- MAGIC - registros duplicados devem ser excluídos ou tratados posteriormente
-- MAGIC
-- MAGIC Essa operação é potencialmente muito mais barata do que verificações completas de tabela para dados que crescem de forma previsível.
-- MAGIC
-- MAGIC Enquanto aqui iremos mostrar a execução simples em um diretório estático, o valor real está em várias execuções ao longo do tempo, selecionando novos arquivos na fonte automaticamente.

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/ecommerce/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
