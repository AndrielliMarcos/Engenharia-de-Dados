-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Criar e compartilhar tabelas no Unity Catalog
-- MAGIC ###Objetivos
-- MAGIC - Criar schemas e tabelas
-- MAGIC - Controlar o acesso dos schema e tabelas
-- MAGIC - Explorar *grants* em vários objetos no Unity Catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Configurar
-- MAGIC Para evitar conflitos em um ambiente de treinamento compartilhado, ao executar a célula abaixo, irá gerar um nome de catálogo exclusivo para seu uso exclusivo.
-- MAGIC
-- MAGIC Em seu próprio ambiente, covê é livre para escolher seus próprios nomes de catálogo, mas tome cuidado para não afetar outros usuários e sistemas nesse ambiente.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-06.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Namespace de três níveis do Unity Catalog
-- MAGIC A maioria dos desenvolvedores de SQL estará familiarizada com o uso de um namespace de dois níveis para endereçar tabelas de forma inequívoca dentro de um schema da seguinte maneira:
-- MAGIC
-- MAGIC     SELECT * FROM schema.table;
-- MAGIC O Unity Catalog apresenta o conceito de um catálogo que reside acima do schema na hierarquia de objetos. Metastores podem hospedar qualquer número de catálogos, que por sua vez podem hospedar qualquer número de schemas. Para lidar com esse nível adicional, as referências de tabelas completas no Unity Catalog usam um namespace de três níveis.
-- MAGIC
-- MAGIC     SELECT * FROM catalog.schema.table;
-- MAGIC
-- MAGIC Os desenvolvedores SQL provavelmente também estarão familiarizados com a instrução **`USE`** para selecionar um schema padrão, para evitar sempre especificar um schema ao fazer referência as tabelas. O Unity Catalog aumenta isso com a instrução **`USE CATALOG`**, que também seleciona um catálogo padrão.
-- MAGIC
-- MAGIC Para simplificar sua experiência, garantimos que o catálogo foi criado e o definimos como padrão, conforme você pode ver no comando a seguir.

-- COMMAND ----------

SELECT current_catalog(), current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar e usar um novo schema
-- MAGIC Vamos criar um novo schema exclusivo para usarmos neste exercício e, em seguida, defini-lo como padrão para que possamos fazer referência às tabelas apenas pelo nome.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS my_own_schema;
USE my_own_schema;

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Criar uma arquitetura Delta
-- MAGIC Vamos criar e popular uma coleção simples de schemas e tabelas de acordo com a arquitetura Delta:
-- MAGIC - um schema *silver* contendo dados de frequência cardíaca do paciente lidos de um dispositivo médico
-- MAGIC - um schema *gold* que calcula a média dos dados de frequência cardíaca por paciente diariamente
-- MAGIC
-- MAGIC Por enquanto, não haverá tabela *bronze* neste exemplo simples.
-- MAGIC
-- MAGIC Observe que precisamos apenas especificar o nome da tabela abaixo, pois definimos um catálogo e schema padrão acima.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS patient_silver;

CREATE OR REPLACE TABLE patient_silver.heartrate (
  device_id  INT,
  mrn        STRING,
  name       STRING,
  time       TIMESTAMP,
  heartrate  DOUBLE
);

INSERT INTO patient_silver.heartrate VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS patient_gold;

CREATE OR REPLACE TABLE patient_gold.heartrate_stats AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM patient_silver.heartrate
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
  
SELECT * FROM patient_gold.heartrate_stats; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Conceder acesso ao schema gold [opcional]
-- MAGIC Agora vamos permitir que usuários do grupo **analysts** leiam do schema **gold**.
-- MAGIC
-- MAGIC Observe que você só pode executar esta seção se tiver seguindo o exercício *Gerenciar usuários e grupos* e criado um grupo no Unity Catalog chamado **analysts**.
-- MAGIC
-- MAGIC Execute esta seção descomentando as céluas de código e executando-as em sequência. Você também será solicitado a executar algumas consultas como um usuário secundário.
-- MAGIC
-- MAGIC Para fazer isso:
-- MAGIC 1. Abra uma sessão de navegação privada separada e faça login no Databricks SQL usando a ID de usuário que você criou ao executar *Gerenciar usuários e grupos* 
-- MAGIC 1. Crie um terminal SQL seguindo as instruções em *Criar terminal SQL no Unity Catalog*
-- MAGIC 1. Prepare-se para inserir consultas conforme as instruções abaixo nesse ambiente

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Vamos conceder o privilégio **SELECT** na tabela **gold**

-- COMMAND ----------

GRANT SELECT ON TABLE patient_gold.heartrate_stats to `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Consultar tabelas
-- MAGIC Com uma concessão **SELECT** em vigor, tente consultar a tabela no ambiente Databricks SQL do seu usuário secundário.
-- MAGIC
-- MAGIC Execute a célula a seguir para gerar uma instrução de consulta que lê da tabela *gold*. Copie e cole a saída em uma nova consulta no ambiente SQL de seu usuário secundário e execute a consulta.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.patient_gold.heartrate_stats")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Isso ainda não funcionará, porque o privilégio **SELECT** na tabela sozinho é insuficiente. O privilégio **USAGE** também é necessário nos elementos recipientes. Vamos corrigir isso agora executando o seguinte.

-- COMMAND ----------

GRANT USAGE ON SCHEMA patient_gold TO `analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Repita a consulta no ambiente Databricks SQL e, com essas duas concessões em vigor, a operação deve ser bem-sucedida.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Explore as GRANTS
-- MAGIC

-- COMMAND ----------

SHOW GRANT ON TABLE ${DA.catalog_name}.patient_gold.heartrate_stats

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Atualmente existe apenas a grant **SELECT** que configuramos anteriormente. Agora vamos verificar as grants em **silver**

-- COMMAND ----------

SHOW TABLES IN ${DA.catalog_name}.patient_silver;

-- COMMAND ----------

SHOW GRANT ON TABLE ${DA.catalog_name}.patient_silver.heartrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Atualmente não há grants nesta tabela, somente o proprietário pode acessar esta tabela.
-- MAGIC
-- MAGIC Agora vamos ver o schema que o contém.

-- COMMAND ----------

SHOW GRANT ON SCHEMA ${DA.catalog_name}.patient_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Atualmente, vemos a grant **USAGE** que configuramos anteriormente.
-- MAGIC
-- MAGIC Agora vamos examinar o catálogo.

-- COMMAND ----------

SHOW GRANT ON CATALOG `${DA.catalog_name}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que o **USAGE** já é concedido aos usuários da conta, e é por isso que não precisamos conceder **USAGE** explicitamente no catálogo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
