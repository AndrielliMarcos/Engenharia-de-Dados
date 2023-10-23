-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Arquitetura

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC O Databricks funciona em um **Control Plane** e um **Data Plane**.
-- MAGIC - Control Plane: inclui os serviços de back-end que o Databricks gerencia na conta da nuvem (Azure, por exemplo). Os comandos de notebook e muitas outras configurações de workspace são armazenados no Control Plane:
-- MAGIC   - Web Aplications;
-- MAGIC   - Repos/notebooks;
-- MAGIC   - Cluster magement.
-- MAGIC   
-- MAGIC   Através do Control Plane e associado a API's que ele fornece, é possível:
-- MAGIC   - iniciar um cluster
-- MAGIC   - iniciar Jobs e obter resultados
-- MAGIC   - interagir com os metadados de uma tabela
-- MAGIC
-- MAGIC - Data Plane: é gerenciado pela conta de nuvem. O Data Plane é o local que os dados residem e é onde os dados são processados.
-- MAGIC   - hospeda os recursos de computação (clusters)
-- MAGIC   - conecta-se ao armazenamento de dados, como o DBFS
-- MAGIC   - opcionalmente fornece conexões a fontes de dados externos
-- MAGIC
-- MAGIC Os resultados interativos do notebook são armazenados em uma combinação do Control Plane (resultados parciais para apresentação na interface do usuário) e no armazenamento da nuvem.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Personas do Databricks
-- MAGIC - Data Science & Engineering
-- MAGIC - Machine Learning
-- MAGIC - SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cluster
-- MAGIC Um cluster é um conjunto de recursos de computação e configuração em que você executa cargas de reabalho de engenharia de dados, ciência de dados e análise de dados, como pipeline de ETL, análise de streaming, entre outros.
-- MAGIC
-- MAGIC As cargas de trabalho podem ser executadas como um conjunto de comandos em um notebook ou como um Job automatizado. 
-- MAGIC
-- MAGIC O Databricks faz uma distinção entre **all-purpose cluster** e o **Job cluster**. O all-purpose cluster é usado para analisar dados de maneira colaborativa usando notebooks interativos. O Job cluster é usado para executar Jobs automatizados rápidos e robustos.
-- MAGIC - O all-purpose cluster pode ser criado usando a interface do usuário, a CLI ou a API Rest. Esse cluster pode ser encerrado e reiniciado manualmente. Vários usuários podem compartilhar tais clusters.
-- MAGIC - **O cluster fica no Data Plane**, dentro da sua conta de nuvem, embora **o gerenciamento do cluster seja uma função do Control Plane**.
-- MAGIC
-- MAGIC O cluster consiste em um conjunto de uma ou mais máquinas virtuais e as cargas de trabalho de computação são distribuídas pelo Apache Spark.
-- MAGIC - as informações de configuração de um Job cluster são retidas para até 30 clusters finalizados recentemente pelo Job scheduler.
-- MAGIC - as informações de configuração de um all-purpose cluster são retidas para té 70 clusters, finalizados dentro de 30 dias. Para reter informações além desse período, o administrador deve fixar o cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Workspace
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O workspace é uma implementação do Databricks na nuvem que funciona como um ambiente para a sua equipe acessar os ativos do Databricks. A organização pode optar por ter vários workspaces ou apenas um, dependemdo das suas necessidades.
