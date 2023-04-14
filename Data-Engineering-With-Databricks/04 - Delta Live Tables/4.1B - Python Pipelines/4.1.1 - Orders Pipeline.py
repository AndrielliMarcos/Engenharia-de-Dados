# Databricks notebook source
# MAGIC %md
# MAGIC #Sintaxe Fundamental Python da DLT
# MAGIC Este notebook demonstra o uso de Delta Live Tables (DLT) para processar dados brutos de arquivos JSON que chegam ao armazenamento de objetos na nuvem por meio de uma série de tabelas para direcionar cargas de trabalho analíticas no lakehouse. Aqui, iremos demonstrar uma arquitetura medalhão, na qual os dados são transformados e enriquecidos de forma incremental à medida que fluem por um pipeline. Este notebook se concentra na sintaxe Python do DLT em vez de se concentrar na arquitetura, mas apresenta uma breve visão geral do projeto:
# MAGIC - A tabela bronze contém registros brutos JSON, enriquecidos com dados que descrevem como os registros foram inseridos
# MAGIC - A tabela silver valida e enriquece os campos de interesse
# MAGIC - A tabela gold contém dados agregados para direcionar insights de negócios e dashboards
# MAGIC 
# MAGIC ###Objetivos:
# MAGIC - Declarar Delta Live Tables
# MAGIC - Ingerir dados com Auto Load
# MAGIC - Usar parâmetros no Pipelile DLT
# MAGIC - Forçar qualidade dos dados com constraints
# MAGIC - Adicionar comentários na tabelas
# MAGIC - Descrever diferentes sintaxes e executar Live Tables e Streaming Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sobre a biblioteca DLT
# MAGIC A sintaxe DLT não se destina à execução interativa em um notebook. Este notebook precisará ser agendado como parte de um pipeline DLT para execução adequada.
# MAGIC 
# MAGIC No momento em que este notebook foi escrito, o tempo de execução atual do Databricks não inclui o módulo dlt, portanto, tentar executar qualquer comando DLT em um notebook falahará.
# MAGIC 
# MAGIC Iremos discutir o desenvolvimento e a solução de problemas do código DLT posteriormente no curso.
# MAGIC 
# MAGIC ###Parametrização
# MAGIC Durante a configuração da pipeline DLT, um número de opções são especificadas. Uma dessa foi o par de chave/valor adicionado para o campo **Configurations**.
# MAGIC 
# MAGIC As configurações no pipeline DLT são similares aos parâmetros no Job Databricks, mas são atualmente definidas como configurações Spark.
# MAGIC 
# MAGIC Em Python, podemos acessar esses valores usando **`spark.conf.get()`**.
# MAGIC 
# MAGIC Ao longo dessas lições, iremos definir a variável Python **`source`** no início do notebook e em seguida usaremos essa variável conforme necessário no código.
# MAGIC 
# MAGIC ###Um nota sobre importações 
# MAGIC O módulo **`dlt`** deve ser explicitamente importado para sua biblioteca no notebook Python.
# MAGIC 
# MAGIC Aqui, devemos importar **`pyspark.sql.functions`** como **`F`**.
# MAGIC 
# MAGIC Alguns desenvolvedores importam **`*`**, enquanto outros irão importar somente a função que eles precisam no presente notebook.
# MAGIC 
# MAGIC Estas lições irão usar **`F`** para que os alunos saibam claramente quais os métodos são importados dessa biblioteca.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tabelas como DataFrames
# MAGIC Existem dois tipos distintos de tabelas persistidas que podem ser criadas com DLT:
# MAGIC * **Live tables** são views materializadas para o lakehouse. Elas irão retornar os resultados atuais de qualquer consulta a cada atualização
# MAGIC * **Streaming live tables** são projetadas para o processamento de dados incremental, quase em tempo real.
# MAGIC 
# MAGIC Observe que ambos desses objetos são mantidos como tabelas armazenadas com o protocolo Delta Lake (fornecendo transações ACID, versionamento e muitos outros benefícios). Iremos falar mais sobre as diferenças entre live tables e streaming live tables posteriomente.
# MAGIC 
# MAGIC Delta Live Tables apresenta uma série de novas functions Python que estendem APIs familiares do PySpark.
# MAGIC 
# MAGIC No centro desse disign, o decorador **`@dlt.table`** é adicionado a qualquer função Python que retorna um Spark DataFrame. (Observação: isso inclui Kaolas DataFrame, mas eles não serão abordados neste curso)
# MAGIC 
# MAGIC Se você está acostumado a trabalhar com Spark e/ou streaming estruturado, você reconhecerá a maior parte da sintaxe usada no DTL. A grande diferença é que você nunca verá nenhum método ou opção para gravar Dataframe, pois ess lógiva é tratada pelo DTL.
# MAGIC 
# MAGIC Então a forma básica de uma definição de tabela DLT será semelhante a:
# MAGIC 
# MAGIC **`@dlt.table`**<br/>
# MAGIC **`def <function-name>():`**<br/>
# MAGIC **`    return (<query>)`**</br>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingestão de Streaming com Auto Loader
# MAGIC O Databricks desenvolveu a funcionalidade Auto Loader para fornecer execução otimizada para carregar dados de forma incremental do armazenamento de objetos em nuvem no Delta Lake. Usar o Auto Loader com DLT é simples: basta configurar um diretório de dados de origem, fornecer alguma definições de configuração e escrever uma consulta no dados de origem. O Auto Loader detectará automaticamente novos arquivos de dados à medida que eles chegarem ao local de armazenamento de objetos na nuvem de origem, processando de forma incremental novos registros sem a necessidade de executar varreduras caras e recalcular resultados para conjuntos de dados em crescimento infinito.
# MAGIC 
# MAGIC O Auto Loader pode ser combinado com APIs de Srteaming estruturado para realizar ingestão de dados incremental em Databricks definindo a configuração **`format("cloudFiles")`**. No DLT, você definirá apenas as configurações associadas à leitura de dados, observando que os locais para inferência e evolução do esquema também serão configurados automaticamente se essas configurações estiverem habilitadas.
# MAGIC 
# MAGIC A consulta abaixo retorna um DataFrame de streaming de uma fonte configurada com o Auto Loader.
# MAGIC 
# MAGIC Além de passar **`cloudFiles`** como formato, especificamos aqui:
# MAGIC * Uma option **`cloudFiles.format`** como **`json`** (isso indica o formato dos arquivos no local de armazenamento do objeto na nuvem)
# MAGIC * Uma option **`cloudFiles.inferColumnTypes`** como **`True`** (para detectar os yipos de cada coluna)
# MAGIC * Uma path do armazenamento do objeto na nuvem para o método **`load`** 
# MAGIC * Uma instrução select que inclui alguns **`pyspark.sql.functions`** para enriquecer os dados ao lado de todos os campos de origem
# MAGIC 
# MAGIC Por padrão, **`@dlt.table`** irá usar o nome da função com o nome da tabela destino.

# COMMAND ----------

@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Validando, enriquecendo e transformando os dados
# MAGIC DLT permite que os usuários declarem tabelas facilmente a partir dos resultados de qualquer transformação padrão do Spark. O DLT adiciona novas funcionalidades para verificações de qualidade de dados e fornece várias opções para permitir que usuários enriqueçam os metadados das tabelas criadas.
# MAGIC 
# MAGIC Vamos detalhar a sintaxe da consulta abaixo.
# MAGIC 
# MAGIC ### Options for **`@dlt.table()`**
# MAGIC Existe um <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-python-ref.html#create-table" target="_blank">número de options</a> que pode ser especificado durante a criação de uma tabela. Aqui, usamos duas dessas para anotar nosso conjunto de dados.
# MAGIC 
# MAGIC ##### **`comment`**
# MAGIC Os comentários da tabela são um padrão para bancos de dados relacionais. Eles podem ser usados para fornecer informações úteis aos usuários em toda a organização. Neste exemplo, escrevemos uma breve descrição legível da tabela que descreve como os dados estão sendo ingeridos e aplicados (o que também pode ser obtido da revisão de outros netadados da tabela).
# MAGIC 
# MAGIC ##### **`table_properties`**
# MAGIC Este campo pode ser usado para passar qualquer número de paras chave/valor para marcação personalizada de dados. Aqui, definimos o valor **`silver`** para a chave **`quality`**.
# MAGIC 
# MAGIC Observe que embora este campo permita que tags personalizadas sejam definidas arbritariamente, ele também é usado para definir várias configurações que controlam o desempenho de uma tabela. Ao revisar os detalhes da tabela, você também pode encontrar várias configurações que são ativadas por padrão sempre que uma tabela é criada.
# MAGIC 
# MAGIC ### Data Quality Constraints
# MAGIC A versão Python do DLT usa funções de decorador para definir restrições de qualidade de dados. Vremos vários deles ao longo do curso.
# MAGIC 
# MAGIC O DLT usa instruções booleanas simples para permitir verificações de imposições de qualidade nos dados. Na declaração abaixo, nós:
# MAGIC - Declaramos uma constraint chamada **`valid_date`**
# MAGIC - Definimos a verificação condicional que o campo **`order_timestamp`** deve conter um valor maior que 01 de janeiro de 2021
# MAGIC - Instruímos o DLT a falhar na transação atual se algum registro violar a restrição usando o decorador **`@dlt.expect_of_fail()`**
# MAGIC 
# MAGIC Cada constraint pode ter várias condições, e várias constrains podem ser definidas para uma única tabela. Além de falhar na atualização, a violação da constraint também pode descartar registros automaticamente ou apenas registrar o número de violações enquanto ainda processa esses registros inválidos.
# MAGIC 
# MAGIC ### Métodos de leitura DLT
# MAGIC O módulo **dlt** Python fornece os métodos **`read`** e o **`read_stream()`** para facilitar a configuração de referências a outras tabelas e views em seu DLT pipeline. Essa sintaxe permite que você faça referência a esses conjuntos de dados por nome sem nenhuma referência de banco de dados. Você também pode usar **`spark.table("LIVE.<table_name.")`**, onde **`LIVE`** é uma palavra chave substituída pelo banco de dados sendo referenciado no DLT pipeline.

# COMMAND ----------

@dlt.table(
    comment = "Append only orders with valid timestamps",
    table_properties = {"quality": "silver"})
@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                "processing_time",
                "customer_id",
                "notifications",
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Live Table vs. Streaming Live Tables
# MAGIC As duas funções que analisamos até agora criaram streaming live tables. Abaixo, vemos uma função simples que retorna uma live table (ou view materializada) de alguns dados agregados.
# MAGIC 
# MAGIC O Spark diferenciou historicamente entre consultas em lote e consultas em streaming. Live tables e streaming live tables têm suas diferenças.
# MAGIC 
# MAGIC Observe que esses tipos de tabela herdam a sintaxe (bem como algumas das limitações) das APIs PySpark e Structured Streaming.
# MAGIC 
# MAGIC Abaixo estão algumas das diferenças entre esses tipos de tabelas:
# MAGIC 
# MAGIC ####Live tables
# MAGIC - sempre "correta", o que signifiva que seu conteúdo corresponderá à sua definição após qualquer atualização
# MAGIC - retorna os mesmos resultados como se a tabela tivesse acabado de ser definida pela primeira vez em todos os dados
# MAGIC - não deve ser modificada por operações externas ao pipeline DLT (você obterá respostas indefinidas ou sua alteração será desfeita)
# MAGIC 
# MAGIC ####Streaming Live Tables
# MAGIC - oferece suporte apenas à leitura de fontes de streaming "somente anexadas"
# MAGIC - lê cada lote de entrada apenas uma vez, não importa o que aconteça (mesmo se as dimensões unidas mudarem ou se a definição da consulta mudas, etc)
# MAGIC - pode executar operações na tabela fora do pipeline DLT gerenciado (acrescentar dados, executar GDPR, etc)

# COMMAND ----------

@dlt.table
def orders_by_date():
    return (
        dlt.read("orders_silver")
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )
