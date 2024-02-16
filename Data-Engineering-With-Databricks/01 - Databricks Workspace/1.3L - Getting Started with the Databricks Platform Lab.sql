-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Getting Started with the Databricks Platform
-- MAGIC Este notebook fornece uma revisão prática de algumas das funcionalidades básicas do Databricks Data Science & Engineering Workspace.
-- MAGIC
-- MAGIC ###Objetivos de aprendizado
-- MAGIC - Renomear um notebook e mudar a linguagem padrão
-- MAGIC - Anexar um cluster
-- MAGIC - Usar o comando mágico **`%run`**
-- MAGIC - Executar células Python e SQL
-- MAGIC - Criar uma célula Markdown

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Renomear um notebook
-- MAGIC Mudar o nome do notebook é fácil. Clique no nome no topo desta página, então faça as mudanças no nome.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Anexar um cluster
-- MAGIC Executar células em um notebook requer recursos computacionais, como os que são fornecidos pelos clusters. A primeira vez que você executar uma célula no notebook, será solicitado anexar um cluster se ainda não tiver anexado.
-- MAGIC
-- MAGIC Anexe um cluster para este notebook agora clicando no dropdown no canto direito desta página. Selecione o cluster criado anteriormente. Isso lipará a execução do notebook e conecta o notebook ao cluster selecionado.
-- MAGIC
-- MAGIC Observe que o menu dropdown fornece opção de iniciar ou reiniciar o cluster conforme necessário. Você também pode desanexar e reanexar um cluster em uma única operação. Isto é útil para limpar o estado de execução quando necessário.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Comando %run
-- MAGIC Projetos complexos de qualquer tipo podem se beneficiar da capacidade de dividí-los em componentes mais simples e reutilizáveis.
-- MAGIC
-- MAGIC Quando usados desta maneira, variáveis, funções e blocos de código se tornão parte do contexto de programação atual.
-- MAGIC
-- MAGIC Considere o exemplo:
-- MAGIC
-- MAGIC **`Notebook_A`** tem quatro comandos:
-- MAGIC   1. **`name = "John"`**
-- MAGIC   2. **`print(f"Hello {name}")`**
-- MAGIC   3. **`%run ./Notebook_B`**
-- MAGIC   4. **`print(f"Welcome back {full_name}`**
-- MAGIC   
-- MAGIC **`Notebook_B`** tem somente um comando:
-- MAGIC    1. **`full_name = f"{name} Doe"`**
-- MAGIC Se nós executarmos o **`Notebook_B`** ele irá falhar a execução porque a variável **`name`** não está definida neste notebook.
-- MAGIC
-- MAGIC Da mesma maneira, alguém pode pensar que **`Notebook_A`** falharia porque usa a variável **`full_name`** que não está definida neste notebook, mas não.
-- MAGIC O que acontece realmente é que os dois notebooks são mesclados como vemos abaixo e então executados:
-- MAGIC   1. **`name = "John"`**
-- MAGIC   2. **`print(f"Hello {name}")`**
-- MAGIC   3. **`full_name = f"{name} Doe"`**
-- MAGIC   4. **`print(f"Welcome back {full_name}")`**
-- MAGIC
-- MAGIC E assim fornece o comportamento esperado:
-- MAGIC   * **`Hello John`**
-- MAGIC   * **`Welcome back John Doe`**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A pasta que contém este notebook contem uma subpasta chamada `ExampleSetupFolder`, que por sua vez contem um notebook chamado exemple-setup.
-- MAGIC
-- MAGIC Este notebook declara a variável `my_name`, definida como `None`, e então cria um Dataframe chamado `example_df`.
-- MAGIC
-- MAGIC Abra o notebook exemple-setup e modifique-o para que `my_name` não seja `None`, mas sim seu nome entre aspas e para que as duas células seguintes sejam executadas sem lançar um **`AssertionError`**.
-- MAGIC
-- MAGIC Você verá referências adicionais **`_utility-methods`** e **`DBAcademyHelper`** que são usadas para este curso de configuração e devem ser ignoradas para este exercício.

-- COMMAND ----------

-- MAGIC %run ./ExampleSetupFolder/example-setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert my_name is not None, "Name is still None"
-- MAGIC print(my_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Executar uma célula Python
-- MAGIC Executar a célula a seguir para verificar que o notebook `exemple-setup` foi executado exibindo o Dataframe `exemple_df`. Esta tabela tem 16 linhas de valores crescentes.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(example_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Alterar a linguagem
-- MAGIC Note que a linguagem padrão deste notebook é definido como Python. Altere para que a linguagem padrão seja SQL.
-- MAGIC
-- MAGIC Observe que as células Python são automaticamente alteradas para o magic command **%python** no início da célula.
-- MAGIC
-- MAGIC Observe também que esta operação limpa o estado de execução das células já executadas.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criar uma célula Markdown
-- MAGIC Adicione uma célula abaixo desta. Popule com alguns markdowns que inclua pelo menos os seguintes elementos:
-- MAGIC * Um título;
-- MAGIC * Tópicos;
-- MAGIC * Um link.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exercício Proposta na célula acima:
-- MAGIC * Uma lista:
-- MAGIC 1. maçã
-- MAGIC 1. banana
-- MAGIC 1. Pera
-- MAGIC
-- MAGIC * Um link: <a href="https://docs.databricks.com/">Documentação</a>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Executar uma célula SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM delta.`${DA.paths.datasets}/nyctaxi-with-zipcodes/data`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Limpando o estado do notebook
-- MAGIC As vezes é útil limpar todas as variáveis definidas no notebook e iniciar do começo. Isso pode ser útil quando você quer testar células isoladas ou você quer simplesmente reiniciar o estado de execução.
-- MAGIC
-- MAGIC Vá até o menu **Run** e selecione **Clear state and outputs**.
-- MAGIC
-- MAGIC Agora tente executar o a célula abaixo e observe que a variável definida anteriormente não são mais definidas, até as células acima serem reexecutadas.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(my_name)
