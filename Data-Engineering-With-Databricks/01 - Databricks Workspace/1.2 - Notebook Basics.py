# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook Basics
# MAGIC Notebooks são o principal meio de desenvolvimento e execução de códigos interativamente no Databricks.
# MAGIC 
# MAGIC Se você havia usado o Databricks notebook anteriormente, mas esta é a primeira vez executando um notebook no Databricks Repos, você vai notar que as funcionalidades básicas são as mesmas.
# MAGIC 
# MAGIC ###Objetivos de Aprendizado:
# MAGIC - Attach(anexar) um notebook no cluster
# MAGIC - Executar uma celula no notebook
# MAGIC - Definir a linguagem do notebook
# MAGIC - Descrever e usar magic commands
# MAGIC - Criar e executar uma célula SQL
# MAGIC - Criar e executar uma célula Python
# MAGIC - Criar uma célula markdown
# MAGIC - Exportar um notebook
# MAGIC - Exportar uma coleção de notebooks do Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anexar um notebook ao cluster
# MAGIC No canto superior direito, é possível acessar os cluster que estão criados e iniciar ou desligar, caso esteja ligado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Noções básicas de notebooks
# MAGIC Notebooks oferecem execução de código célula por célula. Múltiplas linguagens podem ser misturadas no notebooks. Os usuários podem adicionar plots, imagens e textos markdowns para aprimorar seu código.
# MAGIC 
# MAGIC **Obs.:** A execução de código célula a célula significa que as células podem ser executadas multiplas vezes ou fora de ordem. 
# MAGIC 
# MAGIC Os notebooks podem ser facilmente implantados como código de produção com Databricks, assim como fornecer um conjunto de ferramentas robusto para exploração de dados, relatórios e dashboarding.
# MAGIC 
# MAGIC ###Executando uma célula
# MAGIC - A próxima célula pode ser executada usando uma das opções a seguir:
# MAGIC   - **CTRL+ENTER**
# MAGIC   - **SHIFT+ENTER** para executar a célula e passar para a próxima
# MAGIC   - Usando **Run Cell**, **Run All Above** ou **Run All Below** (canto superior direito da célula)

# COMMAND ----------

print("Eu estou executando Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Definindo a linguagem padrão do notebook
# MAGIC Na parte superior do notebook, ao lado do nome deste, pode ser defino a linguagem padrão de todo o notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Eu estou executando em SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Magic Commands
# MAGIC São comandos que nos permite mudar a linguagem de uma célula, independente da linguagem definida para o notebook.
# MAGIC Um único símbolo de porcentagem (%) no início da célula identifica um comando mágico.
# MAGIC - Você pode ter somente um magic command por célula
# MAGIC - Um magic command deve ser a primeira coisa em uma célula
# MAGIC - Usamos o magic command somente quando a linguagem da célula for diferente da linguagem padrão do notebook. Se a linguagem da célula é a mesma do notebook, não é necessário especificar com o magic command

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello Python"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Markdown
# MAGIC O magic command **%md** nos permite apresentar Markdowns na célula
# MAGIC 
# MAGIC #Título um
# MAGIC ##Título dois
# MAGIC ###Título três
# MAGIC 
# MAGIC Este é um texto com uma palavra em **negrito**.
# MAGIC 
# MAGIC Este é um texto com uma palavra em *itálico*.
# MAGIC 
# MAGIC Esta é uma lista ordenada:
# MAGIC 1. um
# MAGIC 1. dois
# MAGIC 1. três
# MAGIC 
# MAGIC Esta é uma lista desordenada:
# MAGIC * maçã
# MAGIC * pera
# MAGIC * banana
# MAGIC 
# MAGIC Links HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC 
# MAGIC Imagens:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC 
# MAGIC E tabelas:
# MAGIC 
# MAGIC | nome   | valor |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# MAGIC %md
# MAGIC ###%run
# MAGIC * Você pode executar um notebook dentro de outro notebook usando o magic command **%run**
# MAGIC * Os notebooks a serem executados são especificados com o caminho relativo.
# MAGIC * Os notebooks referenciados serão executados como se fossem parte do notebook atual.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-01.2

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.schema_name:       {DA.schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.schema_name}' as schema_name

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ###Serviços de utilidades do Databricks
# MAGIC Os notebooks do Databricks fornecem uma série de comandos utilitários para configurar e interagir com o ambiente: <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils docs</a>
# MAGIC 
# MAGIC Use **`dbutils.fs.ls()`** para listar diretórios de arquivos de células Python.

# COMMAND ----------

path = f"{DA.paths.datasets}"
dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###display()
# MAGIC É usado para retornar dados em formato tabular de uma célula Python.
# MAGIC 
# MAGIC O comando **`display()`** tem algumas capacidades e limitações:
# MAGIC * Pré visualização dos resultados é limitado a 1000 registros;
# MAGIC * Fonece botão para download dos dados como CSV

# COMMAND ----------

path = f"{DA.paths.datasets}"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

print(files)

# COMMAND ----------

# comando para apagar tabelas e arquivos associados a esta lição.
DA.cleanup()
