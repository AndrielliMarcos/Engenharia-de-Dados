# Databricks notebook source
# MAGIC %md
# MAGIC #Criar e Gerenciar um Cluster Interativo
# MAGIC Um Databricks cluster é um conjunto de recursos computacionais e configurações em que você executa cargas de trabalho de data engineering, data science e data analytics, como ETL pipelines, streaming analytics, ad-hoc analytics e marchine learning. Você executa essas cargas de trabalho como um conjunto de comandos no notebook ou como um Job automatizado.
# MAGIC
# MAGIC Databricks faz uma distinção entre **all-purpose** e **job cluster**.
# MAGIC - Você usa all-purpose cluster para analisar dados colaborativamente usando notebooks interativos.
# MAGIC - Você usa job cluster para executar jobs automatizados rápidos e robustos.
# MAGIC
# MAGIC Este notebook tem como objetivo mostrar como criar e gerenciar um **cluster all-purpose** usando o Databricks Data Science & Engineering Workspace.
# MAGIC - Use a UI Cluster para configurar e implantar um cluster;
# MAGIC - Edite, termine, reinicie e exclua um cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criar um cluster
# MAGIC Dependendo do workspace no qual você está trabalhando atualmente, você pode ter ou não privilégios de criação de cluster.
# MAGIC
# MAGIC Passos:
# MAGIC 1. Use a barra lateral a esquerda para navegar até o botão **Compute**
# MAGIC 2. Clique no botão **Create Compute**
# MAGIC 3. Para o **cluster name**, use seu nome para que você possa achar o cluster facilmente
# MAGIC 4. Configure o **Cluster mode** para **Single Node**
# MAGIC 5. Use as recomendações do **Databricks runtime version**
# MAGIC 6. Deixe as caixas marcadas para configurações padrão no **Autopilot Options**
# MAGIC 7. Clique no botão **Create Cluster**
# MAGIC
# MAGIC **Obs.:** o cluster pode demorar vários minutos para se implantado. 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Gerenciar um cluster
# MAGIC Uma vez que o cluster é criado, volte para a página **Compute** para visualizar o cluster.
# MAGIC Selecione o cluster criado para rever as configurações atuais.
# MAGIC
# MAGIC Clique no botão **Edit**. Observe que a maioria das configurações podem ser modificadas, se você tiver permissão suficiente. Mudar a maioria das configurações exigirá que o cluster seja reiniciado.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reiniciar, Terminar e Deletar um cluster
# MAGIC
# MAGIC Observe que embora **Restart**, **Terminate** e **Delete** tenham diferentes efeitos, todos eles iniciam com um evento de encerramento de cluster.
# MAGIC (Os clusters também terminarão automaticamente devido a inatividade, assumindo que esta configuração seja usada.)
# MAGIC
# MAGIC Quando um cluster termina, todos os recursos da nuvem em uso são deletados. Isto significa:
# MAGIC - VMs associadas e a memória operacional será limpa;
# MAGIC - O armazenamento anexado será excluído
# MAGIC - As redes de conexão entre os nodes serão removidos
# MAGIC
# MAGIC Em resumo, todos os recursos associados anteriormente com o ambiente computacional será completamente removido. Isto significa que **quaisquer resultados que precisem ser mantidos devem ser salvos em um local permanente**. Observe que você não irá perder seu código, nem irá perder seus arquivos de dados que você tenha salvado apropriadamente.
# MAGIC
# MAGIC O botão **Restart** irá nos permitir reiniciar manualmente nosso cluster. Isso pode ser útil se nós precisarmos limpar completamente o cache no cluster ou desejar reiniciar completamente nosso ambiente computacional.
# MAGIC
# MAGIC O botão **Terminate** nos permite parar nosso cluster. Mantemos nossa definição de configuração do cluster e podemos usar o botão **Restart** para implantar um novo conjunto de recursos de nuvem usando a mesma configuração.
# MAGIC
# MAGIC O botão **Delete** irá parar nosso cluster e remover as configurações do cluster.
