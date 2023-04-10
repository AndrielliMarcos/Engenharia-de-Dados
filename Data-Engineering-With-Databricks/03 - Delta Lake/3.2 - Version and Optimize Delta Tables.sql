-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Versionar e Otimizar Tabelas Delta
-- MAGIC Vamos mostrar alguns recirsos exclusivos do Delta Lake.
-- MAGIC 
-- MAGIC Observe que embora algumas das palavras chaves usadas aqui não são parte do padrão ANSI SQL, todas as operações Delta Lake podem ser executadas no Databricks usando SQL.
-- MAGIC 
-- MAGIC ### Objetivos
-- MAGIC - Usar **`OPTIMIZE`** para compactar arquivos pequenos
-- MAGIC - Usar **`ZORDER`** para indexar tabelas
-- MAGIC - Descrever a estrutura dos diretórios dos arquivos Delta Lake
-- MAGIC - Revisar o hsitórico de transações da tabela
-- MAGIC - Consultar e reverter para a versão anterior da tabela
-- MAGIC - Limpar arquivos de dados obsoletos com **`VACCUM`**
-- MAGIC 
-- MAGIC ### Recursos
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.2 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criando uma tabela Delta com histórico
-- MAGIC A célula abaixo condensa tidas as transações de lição anterior em uma úca célula. (Exceto o comando para dropar a tabela)

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Examine os detalhes da tabela
-- MAGIC 
-- MAGIC O Databricks usa um Hive metastore por padrão para registrar databases, tabelas e views.
-- MAGIC 
-- MAGIC Usando **`DESCRIBE EXTENDED`** para consultar importantes metadados sobre a nossa tabela.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`DESCRIBE DETAIL`** é outro comando que nos permite explorar os metadados da tabela.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe o campo Location.
-- MAGIC 
-- MAGIC Embora até agora tenhamos pensado em nossa tabela apenas como uma entidade relacional dentro de um banco de dados, uma tabela Delta Lake é, na verdade, apoiada por uma coleção de arquivos armazenados no armazenamento de objetos na nuvem.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explorar arquivos Delta Lake
-- MAGIC Podemos ver os arquivos que suportam nossa tabela Delta Lake usando uma função Databricks Utilities.
-- MAGIC 
-- MAGIC **Observação:** Não é importante saber tudo sobre esses arquivos para trabalhar com Delta Lake, mas irá ajudar você a obter uma maior apreciação de como a tecnologia é implementada.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que nosso diretório contém um número de dados de arquivos Parquet e um diretório chamado **`_delta_log`**.
-- MAGIC 
-- MAGIC Os registros nas tabelas Delta Lake são armazenados como dados em arquivos Parquet.
-- MAGIC 
-- MAGIC As transações para as tabelas Delta Lake são registradas no **`_delta_log`**.
-- MAGIC 
-- MAGIC Podemos espiar dentro do **`_delta_log`** para ver mais.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cada transação resulta em um novo arquivo JSON sendo gravado no log de transações do Delta Lake. Aqui, podemos ver que há 8 transoçãoes totais nesta tabela.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Raciocínio sobre arquivos de dados
-- MAGIC Acabamos de ver muitos arquivos de dados para o obviamente é uma tabela muito pequena.
-- MAGIC **`DESCRIBE DETAIL`** nos permite ver alguns outros detalhes sobre nossa Delkta table, incluindo o número de arquivos.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vemos que nossa tabela atual contém 4 arquivos de dados na versão presente. Então, o que todos aqueles outros arquivos Parquet estão fazendo em nosso diretório de tabelas?
-- MAGIC 
-- MAGIC Em vez de substituir ou excluir imediatamente os arquivos que contêm dados alterados, o Delta Lake usa o log de transações para indicar se os arquivos são ou não válidos em uma versão atual da tabela.
-- MAGIC 
-- MAGIC Aqui, veremos o log de transação correspondente à instrução **MERGE** acima, onde os registros foram **inseridos**, **atualizados** e **excluídos**.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A coluna **`add`** contem uma lkista de todos os novos arquivos escritos em nossa tabela. A coluna **`remove`** indica aqueles arquivos que não devem mais ser incluídos em nossa tabela.
-- MAGIC 
-- MAGIC Quando consultamos a tabela Delta Lake, o mecanismo de consulta usa logs de transação para resolver todos os arquivos válidos na versão atual e ignora todos os outros arquivos de dados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Compactando e indexando pequenos arquivos
-- MAGIC Arquivos pequenos podem ocorrer por vários motivos. No nosso caso, realizamos uma série de operações onde apenas um ou vários registros foram inseridos.
-- MAGIC 
-- MAGIC Os arquivos serão combinados para um tamanho ideal (dimensionado com base no tamanho da tabela) usando o comando **`OPTIMIZE`**.
-- MAGIC 
-- MAGIC **`OPTIMIZE`** irá substituir os arquivos de dados existentes combinando registros e reescrevendo os resultados.
-- MAGIC 
-- MAGIC Quando o **`OPTIMIZE`** é executado, usuários podem opcionalmente especificar um ou vários campos por indexação **`ZORDER`**. Embora a matemática específica do **`ZORDER`** não seja importante, ela acelera a recuperação de dados ao filtrar os campos fornecidos, colocando dados com valores semelhantes nos arquivos de dados.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dado o quão pequenos são nossos dados, o **`ZORDER`** não ofecere nenhum benefício, mas podemos ver todas as métricas resultantes dessa operação.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Revisão das transações Delta Lake
-- MAGIC Como todas as alterações na tabela Delta Lake são armazenadas no log de transações, podemos revisar facilmente o histórico da tabela.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como esperado, o **`OPTIMIZE`** criou outra versão da nossa tabela, significando que a versão 8 é a nossa versão mais atual.
-- MAGIC 
-- MAGIC Lembra de todos aqueles arquivos de dados extras que foram marcados como removidos em nosso log de transações? Isso nos fornece a capacidade de consultar versões anteriores de nossa tabela.
-- MAGIC 
-- MAGIC Essa consultas podem ser executadas especificando a versão inteira ou um timestamp.
-- MAGIC 
-- MAGIC **Observação:** na maioria dos casos, você usará um timestamp para recriar dados em um momento de interesse. Para as nossas demonstrações nós usaremos a versão, pois ela é determinística (enquanto você pode executar esta demonstração a qualquer momento no futuro).

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O que é importante observar sobre a consulta de versões passadas é que nós não recriamos um estado anterior da tabela desfazendo transações em nossa versão atual. Em vez disso, estamos apenas consultando todos os arquivos de dados que foram indicados como válidos a partir da versão especificada.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fazer um rollback da versão
-- MAGIC Suponha que você esteja digitando a consulta para excluir manualmente alguns registros de uma tabela e acidentalmente executa essa consulta no seguinte estado.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que quandp vemos um **`-1`** como número de linhas afetadas pelo delete, isto significa que um diretório inteiro de dados foi removido.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Excluir todos os registros em sua tabela provavelmente não é o resultado desejado. Por sorte, podemos simplesmente reverter esse commit.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que o comando **`RESTORE`** é registrado como uma transação. Você não poderá ocultar o fato de ter excluído acidentalmente todos os registros da tabela, mas poderá desfazer a operação e trazer sua tabela de volta ao estado desejado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Limpando arquivos obsoletos
-- MAGIC O Databricks irá limpar automaticamente arquivos obsoletos nas tabelas no Delta Lake.
-- MAGIC 
-- MAGIC Embora a versão do Delta Lake e a recuperação de dados de acordo com o timestamp sejam ótimos para consultar versões recentes e reverter consultas, manter os arquivos de dados para todas as versões de grandes tabelas de produção indefinidamente é muito caro (e pode levar a problemas de conformidade se o PII estiver presente).
-- MAGIC 
-- MAGIC Se você deseja remover arquivos de dados antigos, este pode ser realizado com a operação **`VACUUM`**.
-- MAGIC 
-- MAGIC Descomente a célula a seguir e execute-a com retenção de 0 horas para manter apenas a versão atual:

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Por padrão, o **`VACUUM`** irá impedir que você exclua arquivos com menos de 7 dias, apenas para garantir que nenhuma operação de execução longa ainda faça referência a qualquer um dos arquivos a serem exclupidos. Se você executar **`VACUUM`** em uma tabela Delta, perderá a capacidade de voltar no tempo para uma versão anterior ao período de retenção de dados especificado. Em nossas demonstrações, você pode ver Databricks executando código que especifica uma retenção ) horas. Isso é simplesmente para demonstrar o recurso e normalmente não é feito na produção.
-- MAGIC 
-- MAGIC Na célula a seguir:
-- MAGIC 1. Desativa uma verificação para evitar a exclusão prematura de arquivos de dados
-- MAGIC 1. Certifique-se de que o registro de comandos **`VACUUM`** esteja ativado
-- MAGIC 1. Use a versão **`DRY RUN`** do vacuum para imprimir todos os registros a serem excluídos

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ao executar o **`VACUUM`** e excluir os 10 arquivos acima, removeremos permanentemente o acesso às versões da tabela que exigem a materialização desses arquivos.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Verifique a tabela no diretório para mostrar que os arquivos foram deletados com sucesso.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
