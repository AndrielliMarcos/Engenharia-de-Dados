-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Manipulando Tabelas Delta

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.3L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Recriar o histórico de uma tabela
-- MAGIC Vamos das continuidade ao nosso último laboratório. A célula a seguir condensa todas as operações do último laboratório em uma única célula (além da instrução DROP TABLE final).
-- MAGIC 
-- MAGIC Schema da tabela **`beans`**:
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

-- cria a tabela beans
CREATE TABLE IF NOT EXISTS beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

-- insere os valores
INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

-- atualiza valores
UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

-- deleta valores 
DELETE FROM beans
WHERE delicious = false;

-- cria temp view new_beans
CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

-- MERGE (atualiza ou insere valores na tabela beans a partir da temp view new_beans)
MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Revisão de histórico de tabela
-- MAGIC O log de transações do Delta Lake armazena informações sobre cada transação que modifica o conteúdo ou as configurações de uma tabela.
-- MAGIC 
-- MAGIC Revise o histórico da tabela **`beans`**:

-- COMMAND ----------

-- resposta
DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As operações mostradas com o HISTORY deve ser:
-- MAGIC | version | operation |
-- MAGIC | --- | --- |
-- MAGIC | 0 | CREATE TABLE |
-- MAGIC | 1 | WRITE |
-- MAGIC | 2 | WRITE |
-- MAGIC | 3 | UPDATE |
-- MAGIC | 4 | UPDATE |
-- MAGIC | 5 | DELETE |
-- MAGIC | 6 | MERGE |
-- MAGIC 
-- MAGIC A coluna **`operationsParameters`** permitirá que você revise os predicados usados para atualizações, exclusões e mesclagens. A coluna **`operationMetrics`** indica quantas linhas e arquivos são adicionados em cada operação.
-- MAGIC 
-- MAGIC Passe um tempo revisando o histórico do Delta Lake para entender qual versão da tabela corresponde a uma determinada transação.
-- MAGIC 
-- MAGIC **Observação:** a coluna **version** designa o estado de uma tabela depois que uma determinada transação é concluída. A coluna **`readVersion`** indica a versão da tabela na qual uma operação foi executada. Nesta demostração simples (sem transações simultâneas), este relacionamento deve ser sempre incrementado em 1.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Consultando uma versão específica
-- MAGIC Depois de revisar o histórico da tabela, você decide que quer visualizar o estado da sua tabela depois que seus primeiros dados foram inseridos.
-- MAGIC 
-- MAGIC Execute a consulta abaixo para vê isso.

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC E agora revise o atual estado dos seus dados.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Você quer revisar o peso dos seus feijões antes de deletar qualquer registro.
-- MAGIC 
-- MAGIC Preencha a declaração a seguir para registrar uma temporary view da versão justamente antes que os dados foram deletados, então execute a célula seguinte paa consultar a view.

-- COMMAND ----------

-- resposta
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
  SELECT * FROM beans VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute a célula a seguir para conferir que você tenha capturado a versão correta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Restaurar uma versão anterior
-- MAGIC Aparentemente houve um mal entendido, os feijões que seu amogo lhe deu e que você colocou em sua coleção não eram para ter sido guardados.
-- MAGIC 
-- MAGIC Reverta sua tabela para a versão anterior à conclusão desta instrução **`MERGE`**.

-- COMMAND ----------

-- resposta
RESTORE TABLE beans TO VERSION AS OF 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Revise o histórico da sua tabela. Observe que a restauração para uma versão anterior adiciona outra versão da tabela. 

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Compactação de arquivo
-- MAGIC Observando as métricas de transação durante sua reversão, você fica surpreso por ter tantos arquivos para uma coleção tão pequena de dados.
-- MAGIC 
-- MAGIC Embora seja improvável que a indexação em uma tabela desse tamanho melhore o desempenho, você decide adicionar uma indexação Z-order no campo **`name`** em antecipação ao crescimento exponencial de sua coleção de feijões ao longo do tempo.
-- MAGIC 
-- MAGIC Use a célula a seguir para executar a compactação de arquivo e a indexação Z-order.

-- COMMAND ----------

-- resposta
OPTIMIZE beans
ZORDER BY name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Seus dados devem ter sido compactados para um arquivo único. Confirme isso manualmebnte executando a célula a seguir.

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Limpando os arquivos de dados obsoletos
-- MAGIC Você sabe que embora todos os seus dados residam em um arquivo de dados, os arquivos de dados das versões anteriores da sua tabela ainda estão sendo armazenados junto com ele. Você deseja remover estes arquivos e remover o acesso as versões anteriores da tabela executando o comando **`VACUUM`** na tabela.
-- MAGIC 
-- MAGIC A execução de **`VACUUM`** realiza a limpeza do lixo no diretório da tabela. Por padrão, um limite de retenção de 7 dias será aplicado.
-- MAGIC 
-- MAGIC A célula a seguir modifica algumas configurações do Spark. O primeiro comando substitui a verificação do limite de retenção para nos permitir demonstrar a remoção permanente de dados.
-- MAGIC 
-- MAGIC **Observação:** A limpeza de uma tabela de produção com uma retenção curta pode levar à corrupção de dados e/ou falha de consultas de execução longa. Isso é apenas para fins de demonstração e extremo cuidado deve ser usado ao desabilitar essa configuração.
-- MAGIC 
-- MAGIC O segundo comando **`spark.databricks.delta.vacuum.logging.enabled`** definido como **`true`** para garantir que a operação **`VACUUM`** seja registrada no log de transações.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Antes de deletar os arquivos de dados permanentemente, revise eles manualmente usando a opção **`DRY RUN`**.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Todos os arquivos de dados que não estão na versão atual da tabela serão mostrados na visualização acima.
-- MAGIC 
-- MAGIC Execute o comando novamente sem **`DRY RUN`** para deletar permanentemente estes arquivos.
-- MAGIC 
-- MAGIC **Observação:** Todas as versões anteriores da tabela não estão mais acessíveis.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como o **`VACUUM`** pode ser um ato tão destrutivo para conjuntos de dados importantes, é sempre uma boa ideia ativar novamente a verificação da duração da retenção.
-- MAGIC 
-- MAGIC Execute a célula abaixo para reativar esta configuração.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que o histórico da tabela irá indicar o usuário que concluiu a operação **`VACUUM`**, o número de arquivos deletados e registrará que a verificação de retenção foi desativada durante esta operação.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png">Como o Cache Delta armazena cópias dos arquivos consultados na sessão atual em volumes de armazenamento implantados em seu cluster ativo atualmente, você ainda pode ser capaz de acessar temporariamente versões anteriores da tabela (embora os sistemas não devam ser projetados para esperar esse comportamento).
-- MAGIC 
-- MAGIC Reestartando o cluster irá garantir que esses arquivos de dados em cache sejam permanentemente limpos.
-- MAGIC 
-- MAGIC Você pode ver uma exemplo disso descomentando e executando a célula a seguir que pode, ou não, falhar (dependendo do estado do cache).

-- COMMAND ----------

-- SELECT * FROM beans@v1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
