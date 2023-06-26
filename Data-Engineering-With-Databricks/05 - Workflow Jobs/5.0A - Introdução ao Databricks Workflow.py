# Databricks notebook source
# MAGIC %md
# MAGIC Workflow é o serviço para orquestrar todos os tipos de tarefas: combinação de notebooks, SQL, Spark, modelos de ML e DLT. 
# MAGIC
# MAGIC O Workflow do Databricks têm 2 serviços: as tarefas de Workflow e as DLT. Mas estes 2 serviços podem ser integrados. Por exemplo, um pipeline DLT pode ser executado como uma task workflow.
# MAGIC
# MAGIC Padrões comuns de Workflows:
# MAGIC - **Padrão Sequência:** comum quando temos uma lógica em que todas as tasks precisam ser executadas sequencialmente. Podemos usar este padrão quando queremos passar um dataset de um estado para outro e cada estado precisa esperar o resultado do estado anterior.
# MAGIC - **Padrão Funil:** ótimo quando temos várias fontes de dados e vamos combinar elas em uma ou mais operações que precisam ser concluídas antes de uma tarefa que depende do estado resultante..
# MAGIC - **Padrão de Dispersão:** Quando temos uma única fonte de dados do qual estamos lendo e passamos os dados de ingestão para vários locais de armazenamento de dados diferentes.
# MAGIC
# MAGIC Componentes do Job Workflow:
# MAGIC - **Taks** (what?): notebook, pipeline DLT. arquivo JAR, script Python, etc.
# MAGIC - **Schedule** (When?)
# MAGIC - **Cluster** (How?)
