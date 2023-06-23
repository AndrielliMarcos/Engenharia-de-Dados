# Databricks notebook source
# MAGIC %md
# MAGIC #Usando a interface de usuário da DLT
# MAGIC Esta lição irá explorar a DLT UI.
# MAGIC
# MAGIC ###Objetivos:
# MAGIC - Implantar uma DLT pipeline
# MAGIC - Explorar o resultado DAG
# MAGIC - Executar uma atualização do pipeline

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-04.1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gerar configuração do pipeline
# MAGIC A configuração da nossa pipeline inclui parâmetros exclusivos para um determinado usuário.
# MAGIC
# MAGIC Você irá precisar especificar qual linguagem usar para descomentar a linha apropriada.
# MAGIC
# MAGIC Execute a célula a seguir para exibir os valores usados para configurar seu pipeline nas etapas subsequentes

# COMMAND ----------

#pipeline_language = "SQL"
pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

# MAGIC %md <img src="https://files.training.databricks.com/images/icon_hint_24.png"> **HINT:** Precisaremos consultar os caminhos acima para o notebook #2 e para o notebook #3 em lições posteriores

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criar e configurar a pipeline
# MAGIC 1. Clique em **Workflows**
# MAGIC 1. Selecione a aba **Delta Live Tables**
# MAGIC 1. Clique no botão **Create Pipeline**
# MAGIC 1. No campo **Product edition**, selecione "**Advanced**".
# MAGIC 1. No campo **Pipeline Name**, entre com o valor especificado na célula acima
# MAGIC 1. No campo **Notebook Libraries**, copie a path do Notebook #1 especificando a célula acima e a pasta dele
# MAGIC     * Embora este documento seja um Notebook Databricks padrão, a sintaxe é especializada em declarações de tabela DLT
# MAGIC     * Vamos explorar a sintaxe no próximo execercício
# MAGIC     * Notebooks #2 e #3 serão adicionados nas próximas lições
# MAGIC 1. Configure a origem (Source)
# MAGIC     1. Clique no botão **Add configuration**
# MAGIC     1. No vampo **Key**, digite a palavra "**source**"
# MAGIC     1. No campo **Value**, digite o valor **Source** especificado na célula acima
# MAGIC 1. No campo **Storage location**, digite o valor especificado na célula acima
# MAGIC     * Este campo opcional permite que o usuário especifique um local para armazenar logs, tabelas e outras informações relacionadas à execução do pipeline. Se não for especificado, o DLT gerará automaticamente um diretório
# MAGIC 1. Defina o **Pipeline Mode** para **Triggered**.
# MAGIC     * Este campo especifica como o pipeline será executado
# MAGIC     * **Triggered** os pipelines são executados uma vez e, em seguida, encerrados até a próxima atualização manual ou agendada.
# MAGIC     * **Continuous** os pipelines são executados continuamente, ingerindo novos dados à medida que chegam
# MAGIC     * Escolha o modo com base nos requisitos de latência e custo
# MAGIC 1. Desabilite o autoscaling desmarcando **Enable autoscaling**.
# MAGIC     * **Enable autoscaling**, **Min Workers** e **Max Workers** controla a configuração do worker para o cluster subjacente que processa o pipeline
# MAGIC     * Observe a estimativa de DBU fornecida, semelhante àquela fornecida ao configurar clusters interativos

# COMMAND ----------

# MAGIC %md
# MAGIC Executando no modo local
# MAGIC - Precisamos configurar o pipeline para executar o cluster em modo local
# MAGIC - Na maioria das aplicações, ajustaríamos o número de worker para acomodar a escala do pipeline
# MAGIC - Nossos conjuntos de dados são muito pequenos, estamos apenas prototipando, então podemos usar um cluster de modo local que reduz os custos de nuvem empregando apenas uma única VM
# MAGIC
# MAGIC Configuração modo local:
# MAGIC 1. Nos campos **workers**, defina o valor como **0** - worker e driver usarão a mesma VM
# MAGIC 1. Clique **Add configuration** e em seguida defina a chave para **spark.master** e o valor correspondente para **local[*]**
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Atenção:** Definindo o Workers para 0 e não configurar o **spark.master** resultará em falha na criação do cluster, pois ele estará esperando um worker que não será criado.

# COMMAND ----------

# MAGIC %md
# MAGIC Passos Finais:
# MAGIC 1. Clique no botão criar
# MAGIC 1. Verifique que o pipeline esteja definido no modo "**Development**"

# COMMAND ----------

DA.validate_pipeline_config(pipeline_language)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Executar a Pipeline
# MAGIC Com a pipeline criada, iremos executá-la:
# MAGIC 1. Selecione o modo **Development** para executar o pipeline. Este modo fornece um desenvolvimento iterativo mais ágil reutilizando o cluster (em vez de criar um novo cluster para cada execução) e desativando novas tentativas para que você possa identificar e corrigir erros prontamente. Consulte a <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentação</a> para obter mais informações sobre esse recurso
# MAGIC 1. Clique em **Start**
# MAGIC
# MAGIC A execução inicial levará vários minutos enquanto o cluster é provisionado. As execuções subsequentes serão mais rápidas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Explorando o DAG
# MAGIC A medida que o pipeline é concluído, o fluxo de execução é representado graficamente.
# MAGIC
# MAGIC A seleção das tabelas revisa os detalhes.
# MAGIC
# MAGIC Selecione **orders_silver**. Observe os resultados relatados na seção **Data Quality**. 
# MAGIC
# MAGIC Com cada atualização acionada, todos os dados recém-chegados serão processados por meio de seu pipeline. As métricas sempre serão relatadas para a execução atual.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Carregar outro lote de dados
# MAGIC Execute a célula abaixo para obter mais dados do diretório origem e acione manualmente uma atualização de pipeline.

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC Sempre que necessário durante esta lição você pode retornar neste notebook e usar o método acima para obter novos dados.
