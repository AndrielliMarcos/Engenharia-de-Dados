# Databricks notebook source
# MAGIC %md
# MAGIC #Orquestrando Jobs com Workflows Databricks
# MAGIC Novas atualizações para a interface do usuário de Jobs da Databricks adicionaram a capacidade de agendar várias tasks como parte de um job, permitindo que os jobs do Databricks manioulem totalmente a orquestração para a maioria das vargas de trabalho de produção.
# MAGIC 
# MAGIC Aqui, nós vamos iniciar a revisão das etapas para agendar uma task de notebook como um trabalho autônomo acionado e, em seguida, iremos adicionar uma tarefa dependente usando um pipeline DLT.
# MAGIC 
# MAGIC ###Objetivos:
# MAGIC - Agendar uma task notebook no Job Workflow
# MAGIC - Descrever as opções de agendamento de tasks e as diferenças entre os tipos de cluster
# MAGIC - Revisar as execuções de trabalho para acompanhar o progresso e ver os resultados
# MAGIC - Agendar uma task pipeline DLT no Job Workflow
# MAGIC - Configurar dependências lineares entre tasks usndo a interface do usuário do Workflow

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-05.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agendar um Job Notebook
# MAGIC Ao usar a interface de usuários para orquestrar uma carga de trabalho com várias tasks, você sempre irá começar agendando uma única task.

# COMMAND ----------

# valores que usaremos nesta etapa
DA.print_job_config_v1()

# COMMAND ----------

# MAGIC %md
# MAGIC Aqui, iremos começar agendando o próximo notebook.
# MAGIC 
# MAGIC Passos:
# MAGIC 1. Clique no botão **Workflows** na barra lateral.
# MAGIC 1. Selecione a aba **Jobs**.
# MAGIC 1. Clique no botão **Create Job**.
# MAGIC 1. Configure a task:
# MAGIC     1. Digite **Reset** para o nome da task
# MAGIC     1. Para **Type**, selecione **Notebook**
# MAGIC     1. Para **Path**, selecione o valor **Reset Notebook Path** fornecido na célula acima
# MAGIC     1. No menu suspenso **Cluster**, em **Existing All Purpose Clusters**, selecione seu cluster
# MAGIC     1. Clique **Create**
# MAGIC 1. No topo esquerdo da tela, renomeie o Job (não a task) de **`Reset`** (valor padrão) para o valor **Job Name** fornecido na célula acima.
# MAGIC 1. Clique no botão azul **Run now** no topo direito para iniciar o job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Observação**: Quando selecionar seu cluster all-purpose, você receberá um aviso sobre como isso será cobrado como computação all-purpose. Os jobs de produção sempre devem ser agendados em novos Job clusters dimensionados adequadamente para a carga de trabalho, pis isso é cobrado a uma taxa muito menor.

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agendamento de Jobs Databricks
# MAGIC Observe que no lado direito da interface do usuário do Job, logo abaixo da seção **Job Details**, há uma seção denominada **Schedule**.
# MAGIC 
# MAGIC Clique no botão ** Edit schedule** para explorar as opções de agendamento.
# MAGIC 
# MAGIC Alterar o campo **Schedule type** de **Manual** para **Scheduled** irá exibir uma interface de usuário de agendamento.
# MAGIC 
# MAGIC Essa interface de usuário oferece opções abrangentes para configurar o agendamento cronológico de seus Jobs. As configurações definidas com a interface do usuário também podem ser geradas na sintaxe do cron, que pode ser editada se for necessária uma configuração personalizada não disponível com a interface do usuário.
# MAGIC 
# MAGIC Neste momento, deixaremos nosso trabalho configurado para o tipo de agendamento **Manual (Pausado)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Revisão da Execução
# MAGIC Conforme configurado atualmente, nosso único notebook fornece desempenho idêntico à interface de usuário de Jobs do Databricks herdada, que permitia agendar apenas um único notebook.
# MAGIC 
# MAGIC Para revisar a exevução do Job:
# MAGIC 1. Selecione a aba **Runs** no topo esquerdo da tela (você deve estar na aba **Tasks**)
# MAGIC 1. Procure seu job. Se **the job is still running**, ele estará na seção **Active runs**. Se **the job finished running**, ele estará na seção**Completed runs**
# MAGIC 1. Abra os detalhes da saída clicando no campo timestamp na coluna **Start time** 
# MAGIC 1. Se **the job is still running**, você verá o estado ativo do notebook com um **Status** de **`Pending`** ou **`Running`** no painel do lado direito. Se **the job has completed**, você verá a execução completa do notebook com um **Status** de **`Succeeded`** ou **`Failed`** no painel do lado direito.
# MAGIC 
# MAGIC O notebook emprega o magic command **`%run`** para chamar um notebook adicional usando uma path relativa. Observe que, embora não seja abordado neste curso, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">a nova funcionalidade adicionada ao Databricks Repos permite carregar módulos Python usando paths relativos</a>. 
# MAGIC 
# MAGIC O resultado real do notebook agendado é redefinir o ambiente para nosso novo Job e pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agendar um pipeline DLT como uma Task
# MAGIC O pipeline que criamos aqui é uma versão semplificada da unidade anterior.
# MAGIC 
# MAGIC Vamos usá-lo como parte de um trabalho agendado nesta etapa.
# MAGIC 
# MAGIC Para que possamos nos concentrar em Jobs e não em Pipelines, vamos usar o seguinte comando de utilitário para criar o pipeline para nós.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC Em seguida, precisamos adicionar a tarefa para executar esse pipeline.
# MAGIC 
# MAGIC Passos:
# MAGIC 1. No topo esquerdo da tela, você verá que a aba **Runs** está selecionada no momento; clique na aba **Tasks**.
# MAGIC 1. Clique no grande círculo azul com um **+** na parte inferior central da tela para adicionar uma nova tarefa.
# MAGIC 1. Configure a task:
# MAGIC     1. Digite **DLT** para o nome da task
# MAGIC     1. Para **Type**, selecione  **Delta Live Tables pipeline**
# MAGIC     1. Para **Pipeline**, selecione o DLT pipeline que você configurou anteriormente<br/>
# MAGIC     Observação: A pipeline irá iniciar com **DLT-Job-Demo-61** e irá finalizar com seu endereço de email.
# MAGIC     1. O campo **Depends on** padrão para a sua task definida anteriormente, **Reset** - deixe este valor como está.
# MAGIC     1. Clique no botão azul **Create task**.
# MAGIC   
# MAGIC Agora você deve ver uma tela com 2 caixas e uma seta para baixo entre elas.
# MAGIC 
# MAGIC Sua tarefa **`Reset`** estará no topo, levando à sua task **`DLT`**.
# MAGIC 
# MAGIC Esta visualização representa as dependências entre estas tasks.
# MAGIC 
# MAGIC Clique **`Run now`** para executar seu job.
# MAGIC 
# MAGIC **Observação:** Pode ser necessário aguardar alguns minutos enquanto a infraestrutura para seu Job e o pipeline são implantados.

# COMMAND ----------

DA.validate_job_v2_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Revise os resultados da execução de Multi-Task
# MAGIC Selecione a aba **`Runs`** novamente e,em seguida, a execução mais recente em **`Active runs`** ou **`Completed runs`** dependendo se o Job foi concluído ou não.
# MAGIC 
# MAGIC As visualizações das tarefas serão atualizadas em tempo real para refletir quais tarefas estão sendo executadas ativamente e mudarão de cor se ocorrere, falhas nas tarefas.
# MAGIC 
# MAGIC Clicar em uma caixa de tarefa renderizará o notebook agendado na interface do usuário.
# MAGIC 
# MAGIC Você pode pensar nisso apenas como uma camada adicional de orquestração sobre a interface do usuário anterior do Databrocks Job, se isso ajudar. Observe que, se você tiver cargas de trabalho agendando Jobs com VLI ou API REST, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">a estrutura JSON usada para configurar e obter resultados sobre trabalhos recebeu atualizações semelhantes na interface do usuário</a>.
# MAGIC 
# MAGIC **Observação:** no momento, os pipelines DLT agendados como tasks não renderizam resultados diretamente na GUI de exevuções. Em vez disso, você será direcionado de volta à GUI do pipeline DLT para o pipeline agendado.
