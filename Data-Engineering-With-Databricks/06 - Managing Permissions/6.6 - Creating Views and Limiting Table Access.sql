-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Criar views e limitar o acesso a tabelas
-- MAGIC ###Objetivos:
-- MAGIC - Criar views
-- MAGIC - Gerenciar o acesso as views
-- MAGIC - Usar o recurso dynamic view para restringir o acesso a colunas e linhas dentro da tabela

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Configurar
-- MAGIC Execute a célula a seguir para fazer algumas configurações. Para evitar conflitos em um ambiente de treinamento compartilhado, isso irá criar um database com nome exclusivo para o seu uso.. Isso também irá criar uma tabela de exemplo chamada **silver** dentro do metastore do Unity Catalog.
-- MAGIC 
-- MAGIC Observação: este notebook assume um catálogo chamado *main* em seu metastore do Unity Catalog. Se você precisar direcionar para um catálogo diferente, edite o notebook em **Classroom-Setup**.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-06.6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos examinar o conteúdo da tabela **silver**.
-- MAGIC 
-- MAGIC Observação: como parte da configuração, um catálogo e um database padrão foram selecionados, portanto precisamos especificar somente os nomes da tabelas e views sem níveis adicionais.

-- COMMAND ----------

SELECT * FROM silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar a view gold
-- MAGIC Com a tabela *silver* definida, vamos criar uma view que agregue dados da *silver*, apresentando dados adequados para a camada *gold* de uma arquitetura medalhão.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_dailyavg AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos examinar a view gold.

-- COMMAND ----------

SELECT * FROM gold_dailyavg

-- COMMAND ----------

SHOW GRANT ON VIEW gold_dailyavg

-- COMMAND ----------

SHOW GRANT ON TABLE silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder acesso à view [opcional]
-- MAGIC Com uma nova view em vigor, vamos permitir que os usuários do grupo *analysts* a consultem.
-- MAGIC 
-- MAGIC Observe que você só pode executar esta seção se tiver seguido o exercício *Gerenciar usuários e grupos* e criado um grupo no Unity Catalog chamado *analysts*.
-- MAGIC 
-- MAGIC Execute esta seção descomentando as céluas de código e executando-as em sequência. Você també será solicitado a executar algumas consultas como um usuário secundário. 
-- MAGIC Para fazer isso:
-- MAGIC 1. Abra uma sessão de navegação privada separada e faça login no Databricks SQL usando a ID de usuário que você criou ao executar *Gerenciar usuários e grupos* 
-- MAGIC 1. Crie um terminal SQL seguindo as instruções em *Criar terminal SQL no Unity Catalog*
-- MAGIC 1. Prepare-se para inserir consultas conforme as instruções abaixo nesse ambiente

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder privilégio SELECT na view
-- MAGIC A primeira instrução é para conceder privilégio **SELECT** na view para o grupo **analysts**.

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold_dailyavg to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder privilégio USAGE no database
-- MAGIC Como nas tabelas, o privilégio **USAGE** também é necessário no database para consultar a view.

-- COMMAND ----------

-- GRANT USAGE ON DATABASE `${da.schema_name}` TO `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Consultar a view como usuário
-- MAGIC Com os acessos apropriados em vigor, tente consultar a view no ambiente Databricks SQL do seu usuário secundário.
-- MAGIC 
-- MAGIC Execute a célula a seguir para gerar uma instrução de consulta que lê da view. Copie e cole a saída em uma nova consulta no ambiente SQL de seu uau´srio secundário e execute a consulta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.{DA.schema_name}.gold_dailyavg")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que a consulta foi bem-suvedida e a saída é idêntica à saída acima, conforme esperado.
-- MAGIC 
-- MAGIC Agora substitua **`gold_dailyavg`** por **`silver`** e execute novamente a consulta. Observe que a consulta agora falha. Isso ocorre porque o usuário não tem privilégio **SELECT** na tabela **`silver`**.
-- MAGIC 
-- MAGIC Lembre-se porém que **`gold_dailyavg`** é uma view que seleciona da **`silver`**. Então como a consulta **`gold_dailyavg`** pode ser bem-secedida? O Unity Catalog permite que a consulta passe porque o proprietário dessa view tem privilégio **SELECT** em **`silver`**. Esta é uma propriedade importante, pois nos permite implementar views que podem filtrar ou mascarar linhas ou colunas de uma tabela, sem permitir acesso direto à tabela subjacente que estamos tentando proteger. Veremos esse mecanismo em ação a seguir.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Dynamic views
-- MAGIC As views dinâmicas nos permitem configurar um controle de acesso mais refinado, incluindo:
-- MAGIC - segurança a nível de linhas e colunas
-- MAGIC - mascaramento de dados
-- MAGIC 
-- MAGIC O controle de acesso é obtido através do uso de funções dentro da definição da view. Essa funções incluem:
-- MAGIC * **`current_user()`**: retorna o endereço de email do usuário atual
-- MAGIC * **`is_account_group_member()`**: retorna TRUE se o usuário atual é membro do grupo especificado 
-- MAGIC 
-- MAGIC Observação: para compatibilidade herdada, também existe a função **`is_member()`** que retorna TRUE se o usuário atual for membro do grupo a nível do workspace especificado. Evite usar ess função ao implementar views dinâmicas no Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Restringir colunas
-- MAGIC Vamos aplicar **`is_account_group_member()`** para mascarar colunas contendo PII para membros do grupo de *analysts* por meio de instruções **CASE** dentro do **SELECT**.
-- MAGIC 
-- MAGIC Observação: este é um exemplo simples para alinhar com a configuração deste ambiente de treinamento. Em um sistema de produção, o método preferível seria restringir as linhas para usuários que não são membros de um grupo específico.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_dailyavg AS
SELECT
  CASE WHEN
    is_account_group_member('analysts') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('analysts') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos reemitir a concessão na view atualizada.

-- COMMAND ----------

GRANT SELECT ON VIEW gold_dailyavg to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos consultar a view, que produzirá uma saída não filtrada (supondo que o usuário atual não tenha sido adicionado ao grupo de analistas).

-- COMMAND ----------

SELECT * FROM gold_dailyavg

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora execute novamente a consulta que você executou anteriormente no ambiente Databricks SQL (alterando **silver** de volta para **`gold_dailyavg`**). Observe que o PII agora está filtrado. Não há como os membros dess grupo obter acesso às PII, pois elas estão sendo protegidas pela view e não há acesso direto à tabela subjacente.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Restringir linhas
-- MAGIC Vamos agora aplicar **`is_account_group_member()`** para filtrar as linhas. Neste caso, criaremos uma nova view *gold* que retorna o timestamp e o valor da frequência cardíacam restritos aos membros do grupo *analysts*, para linhas cujo **device id** seja menor que 30. A filtragem de linha pode ser feita aplivando a condicional como uma cláusula **WHERE** no **SELECT**.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_allhr AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('analysts') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold_allhr to `analysts`

-- COMMAND ----------

SELECT * FROM gold_allhr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora execure novamente a consulta executada anteriormente no ambiente Databricks SQL (alterando **`gold_dailyavg`** para **`gold_allhr`**). Observe que as linhas dujo ID de dispositivo é 30 ou maior são omitidas da saída.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Mascaramento de dados (Data masking)
-- MAGIC Um caso de uso final para as views dinâmicas é mascarar dados. Ou seja, permitir a passagem de um subconjunto de dados, mas transformá-lo de forma que a totalidade do campo mascarado não possa ser deduzida.
-- MAGIC 
-- MAGIC Aqui, combinamos a abordagem de filtragem de linha e coluna para aumentar nossa filtragem de linha com o mascaramento de dados na view. Mas, em vez de substituir a voluna inteira pela string **REDACTED**, utilizamos a função de manipulação de string SQL para exibir os dois últimos dígitos do **mrn**, enquanto mascaramos o restante.
-- MAGIC 
-- MAGIC Dependendo de suas necessidades, o SQL fornece uma biblioteca bastante abrangente de funções de manipulação de strings que podem ser aproveitadas para mascarar dados de várias maneiras diferentes. A abordagem mostrada abaixo ilustra um exemplo simples disso.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_allhr AS
SELECT
  CASE WHEN
    is_account_group_member('analysts') THEN CONCAT("******", RIGHT(mrn, 2))
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('analysts') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold_allhr to `analysts`

-- COMMAND ----------

SELECT * FROM gold_allhr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Execute novamente a consulta em **`gold_allhr`** uma última vez no ambiente Databricks SQL. Observe que, além de algumas linhas serem filtradas, a coluna **mrn** é mascarada de forma que apenas os dois últimos dígitos sejam exibidos. Isso fornece informações suficientes para correlacionar registros com pacientes conhecidos, mas por si só não divulga nenhuma PII.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
