-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Atualizar uma tabela para o Unity Catalog
-- MAGIC ###Objetivos:
-- MAGIC - Migrar uma tabela existente do Hive metastore para o Unity Catalog
-- MAGIC - Criar GRANTS apropriadas para permitir que outras pessoas acessem a tabela
-- MAGIC - Realizar transformações simples em uma tabela enquanto migra para o Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Configurar
-- MAGIC Execute as células a seguir para realizar algumas configurações. Para evitar conflitos em um ambiente de treinamento compartilhado, isso criará um banco de dados com nome exclusivo para seu uso. Isso também criará uma tabela de origem de exemplo chamada filmes no metastores legado do Hive.
-- MAGIC
-- MAGIC Observação: este notebook assume um catálogo chamado *main* em seu metastore do Unity Catalog. Se você precisar direcionar para um catálogo diferente, edite o notebook em **Classroom-Setup**.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-06.5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No metastore herdado do Hive local para este workspace, agora temos uma tabela chamada **movies**, que está em um banco de dados específico do usuário na saída da célula acima. Para facilitar as coisas, o nome do bando de dados é armazenado em uma variável do Hive chamada *da.schema_name*. Vamos visualizar os dados nesta tabela usando essa variável.

-- COMMAND ----------

SELECT * FROM hive_metastore.`${DA.schema_name}`.movies LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Configurar o destino
-- MAGIC Com uma tabela de origem definida, vamos configurar um destino no Unity Catalog para o qual migrar nossa tabela.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Selecionar o metastore para uso do Unity Catalog
-- MAGIC Vamos começar selecionando um catálogo do mestore do Unity Catalog. Por padrão, ele é definido como **main**, a menos que você tenha editado o notebook **Classroom-Setup**. Não é necessário fazer isso, mas elimina a necessidade de especificar um catálogo em sua referências de tabela.

-- COMMAND ----------

USE CATALOG `${DA.catalog_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar e selecionar banco de dados para uso
-- MAGIC Vamos criar um banco de dados exclusivo no Unity Catalog para evitar possíveis interferências com outras pessoas em um ambiente de treinamento compartilhado. Para fazer isso, aproveitaremos a variável Hive definida anteriormente.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS `${DA.schema_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos selecionar o banco de dados recém-criado para simplificar ainda mais o trabalho com a tabela de destino. Novamente, esta etapa não é necessária, mas simplificará ainda mais as referências às suas tabelas atualizadas.

-- COMMAND ----------

USE `${DA.schema_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Atualizar tabela
-- MAGIC Copiar a tabela se resume a uma simples operação **CREATE TABLE AS SELECT (CTAS)**, usando o namespace de três níveis para especificar a tabela de origem. Não precisamos especificar o destino usando três níveis devido às instruções **USE CATALOG** e **USE SCHEMA** executadas anteriormente.

-- COMMAND ----------

CREATE OR REPLACE TABLE movies
AS SELECT * FROM hive_metastore.`${DA.schema_name}`.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A tabela agora está copiada. Com a nova tabela sob o controle do Unity Catalog, vamos verificar rapidamente se há permissões concedidas na nova tabela.

-- COMMAND ----------

SHOW GRANTS ON movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Atualmente não há GRANTS.
-- MAGIC
-- MAGIC Agora vamos examinar as GRANTS na tabela original. Descomente o código na célula a seguir e execute-o.

-- COMMAND ----------

-- SHOW GRANTS ON hive_metastore.`${DA.schema_name}`.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Isso gera um erro, pois esta tabela reside no metastore herdado e não estamos executando em um cluster com controle de acesso à tabela herdada. Isso destaca um benefício importante do Unity Catalog: nenhuma configuração adicional é necessária para obter uma solução segura. O Unity Catalog é seguro por padrão.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder acesso à tabela [opcional]
-- MAGIC Com uma nova tabela em vigor, vamos permitir que os usuários do grupo *analysts* leiam.
-- MAGIC
-- MAGIC Observe que você só pode executar esta seção se tiver seguido o exercício *Gerenciar usuários e grupos* e criado um grupo no Unity Catalog chamado *analysts*.
-- MAGIC
-- MAGIC Execute esta seção descomentando as céluLas de código e executando-as em sequência. Você tambéM será solicitado a executar algumas consultas como um usuário secundário. 
-- MAGIC Para fazer isso:
-- MAGIC 1. Abra uma sessão de navegação privada separada e faça login no Databricks SQL usando a ID de usuário que você criou ao executar *Gerenciar usuários e grupos* 
-- MAGIC 1. Crie um terminal SQL seguindo as instruções em *Criar terminal SQL no Unity Catalog*
-- MAGIC 1. Prepare-se para inserir consultas conforme as instruções abaixo nesse ambiente

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder privilégio SELECT na tabela
-- MAGIC O primeiro requisito é conceder o priVilégio **SELECT** na nova tabela ao grupo *analysts*.

-- COMMAND ----------

GRANT SELECT ON TABLE movies to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder privilégio USAGE no database
-- MAGIC O privilégio USAGE também é necessário no database.

-- COMMAND ----------

GRANT USAGE ON DATABASE `${DA.schema_name}` TO `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Acessar tabela como usuário
-- MAGIC Tente ler a tabela no ambiente Databricks SQL do seu usuário secundário.
-- MAGIC
-- MAGIC Execute a célula a seguir para gerar uma instrução de consulta que lê a tabela recém-criada. Copie e cole a saída em uma nova consulta no ambiente SQL de seu usuário secundário e execute a consulta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.{DA.schema_name}.movies")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Transforme a tabela durante a atualização
-- MAGIC A migração de uma tabela para o Unity Catalog é uma operação simples, mas a mudança geral para o Unity Catalog é importante para qualquer organização. É um ótimo momento para considerar cuidadosamente suas tabelas e schemas e se eles ainda atendem aos requisitos de negócios de sua organização que podem ter mudado ao longo do tempo.
-- MAGIC
-- MAGIC O exemplo que vimos anteriormente faz uma cópia exata da tabela origem. Como a migração de uma tabela é uma operação simples de **CREATE TABLE AS SELECT**, podemos realizar quaiquer transformações durante a migração que podem ser realizadas com **SELECT**. Por exemplo, vamos expandir o exemplo anterior para fazer as seguintes transformações:
-- MAGIC - Atribua o nome *idx* à primeira coluna
-- MAGIC - Selecione apenas as colunas **title**, **year**, **budget** e **rating**
-- MAGIC - Converta **year** e **budget** para **INT**
-- MAGIC - Converta **rating** para **double**

-- COMMAND ----------

CREATE OR REPLACE TABLE movies
AS SELECT
  _c0 AS idx,
  title,
  CAST(year AS INT) AS year,
  CASE WHEN
    budget = 'NA' THEN 0
    ELSE CAST(budget AS INT)
  END AS budget,
  CAST(rating AS DOUBLE) AS rating
FROM hive_metastore.`${da.schema_name}`.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Se você estiver executando consultas como um usuário secundário, execute novamente a consulta anterior no ambiente Databricks SQL do usuário secundário. Verifique se:
-- MAGIC 1. a tabela ainda pode ser acessada
-- MAGIC 1. o schema da tabela foi atualizado

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
