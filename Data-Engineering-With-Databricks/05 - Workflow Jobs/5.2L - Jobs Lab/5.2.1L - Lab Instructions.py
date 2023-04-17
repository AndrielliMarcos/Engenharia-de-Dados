# Databricks notebook source
# MAGIC %md
# MAGIC #Laboratório: Orquestrando Jobs com Databricks
# MAGIC Neste laboratório, iremos configurar um Job multi-task composto por:
# MAGIC - Um notebook que coloca um novo lote de dados em um diretório de armazenamento
# MAGIC - Um pipeline DLT que processa esses dados por meio de uma série de tabelas
# MAGIC - Um notebook que consulta a tabela gold produzida por ess pipeline, bem como várias métricas geradas por DLT
# MAGIC 
# MAGIC ###Objetivos
# MAGIC - Agendar um notebook como uma task no Job Databriks
# MAGIC - Agendar uma pipeline DLT como uma task no Job Databricks
# MAGIC - Configurar dependências lineares entre as tasks usando a interface de usuário Workflows Databricks

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-05.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dados inicial
# MAGIC Coloque alguns dados na camada landing.
# MAGIC 
# MAGIC Você executará novamente este comando para obter dados adicionais posteriormente.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agendar um Job tipo notebook
# MAGIC Quando usamos a inteface de usuário do Job para orquestrar uma carga de trabalho com múltiplas tasks, você irá começar sempre agendando uma task única.
# MAGIC 
# MAGIC Execute a célula abaixo para pegar os valores usados nesta etapa.

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC Aqui, iremos iniciar agendamdo o primeiro notebook.
# MAGIC 
# MAGIC Etapas:
# MAGIC 1. Clique no botão **Workflows** na barra lateral
# MAGIC 1. Selecione a aba **Jobs**.
# MAGIC 1. Clique no botão azul **Create Job**
# MAGIC 1. Configure a task:
# MAGIC     1. Digite **Batch-Job** para o nome da task
# MAGIC     1. Para **Type**, selecione **Notebook**
# MAGIC     1. Para **Path**, selevione o valor **Batch Notebook Path** fornecido na célula acima
# MAGIC     1. No menu suspenso **Cluster**, em **Existing All Purpose Clusters**, selecione seu cluster
# MAGIC     1. Clique **Create**
# MAGIC 1. No topo esquerdo da tela, renomeie o Job(não a task) de **`Batch-Job`** (valor padrão) para **Job Name** valor fornecido na célula acima
# MAGIC 1. Clique no botão **Run now** no topo direito para iniciar o job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: Quando selecionar seu cluster all-purpose, você receberá um aviso sobre como isso será cobrado como computação all-purpose. Os jobs de produção sempre devem ser agendados em novos Job clusters dimensionados adequadamente para a carga de trabalho, pis isso é cobrado a uma taxa muito menor.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agendar uma pipeline DLT como uma Task
# MAGIC Nesta etapa, iremos adicionar uma pipeline DLT para executar após o sucesso da task que configuramos no início desta lição.
# MAGIC 
# MAGIC Para que possamos nos concentrar em Jobs enão em pipelines, usaremos o seguinte comando de utilitário para criar o pipeline para nós.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC Etapas:
# MAGIC 1. No topo esquerdo da tela, você verá que a aba **Runs** está selecionada no momento; clique na aba **Tasks**.
# MAGIC 1. Clique no grande círculo azul com um **+** na parte inferior central da tela para adicionar uma nova tarefa.
# MAGIC 1. Configure a task:
# MAGIC     1. Digite **DLT** para o nome da task
# MAGIC     1. Para **Type**, selecione  **Delta Live Tables pipeline**
# MAGIC     1. Para **Pipeline**, selecione o DLT pipeline que você configurou anteriormente    
# MAGIC     1. O campo **Depends on** padrão para a sua task definida anteriormente, **Batch-Job** - deixe este valor como está.
# MAGIC     1. Clique no botão azul **Create task**.
# MAGIC     
# MAGIC Agora você deve ver uma tela com 2 caixas e uma seta para baixo entre elas.
# MAGIC 
# MAGIC Sua tarefa **`Batch-Job`** estará no topo, levando à sua tarefa **`DLT`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agendando uma task adicional do tipo notebook
# MAGIC Foi fornecido um notebook adicional que consulta algumas das métricas DLT e a tabela Gold definida no pipeline DLT.
# MAGIC 
# MAGIC Vamos adicionar isso como uma tarefa final em nosso trabalho.
# MAGIC 
# MAGIC Etapas:
# MAGIC 1. Clique no grande botão azul com um **+** no centro da tela para adicionar uma nova task
# MAGIC 1. Configure a task:
# MAGIC     1. Digite **Query-Resukts** para o nome da task
# MAGIC     1. Para **Type**, selecione **Notebook**
# MAGIC     1. Para **Path**, selecione o valor **Query Notebook Path** fornecido na célula acima
# MAGIC     1. No menu suspenso **Cluster**, em **Existing All Purpose Clusters**, selecione seu cluster
# MAGIC     1. O campo **Depends on** padrão para a sua task definida anteriormente, **DLT** - deixe este valor como está.
# MAGIC     1. Clique no botão azul **Create task**
# MAGIC 
# MAGIC Clique no botão **Run now** no topo direito da tela para executar este job.
# MAGIC 
# MAGIC Na aba **Runs**, você poderá clicar na hora de início desta execução na seção **Active runs** e acompanhar vicualmente o andamento da tarefa.
# MAGIC 
# MAGIC Depois que todas as suas tarefas forem bem-sucedidas, revise o conteúdo de cada tarefa para confirmar o comportamento esperado.

# COMMAND ----------

DA.validate_job_config()
