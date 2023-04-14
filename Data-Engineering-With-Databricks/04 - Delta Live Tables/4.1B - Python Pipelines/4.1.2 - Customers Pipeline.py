# Databricks notebook source
# MAGIC %md
# MAGIC #Mais Syntaxe DLT Python
# MAGIC Os pipelines DLT facilitam a combinação de vários conjuntos de dados em uma única carga de trabalho escalonável usando um ou mais notebooks.
# MAGIC 
# MAGIC No último notebook, revisamos algumas das funcionalidades básicas da sintaxe DLT durante o processamento de dados do armazenamento de objetos na nuvem por meio de uma série de consultas para validar e enriquecer os registros em cada etapa. Este notebook, também segue a arquitetura medalhão, mas apresenta uma série de novos conceitos.
# MAGIC 
# MAGIC - registros brutos representam informações de change data capture(CDC) sobre clientes
# MAGIC - a tabela bronze usa novamente o Auto Loader para ingerir dados JSON do armazenamento de objetos na nuvem
# MAGIC - uma tabela é definida para impor restrições antes de passar resgistros para a camada silver
# MAGIC - **`dlt.apply_changes()`** é usado para processar automaticamente os dados do CDC na camada silver como uma tabela de <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">slowly changing dimension<a/> (SCD - dimensão de mudança lenta) Tipo 1
# MAGIC - uma tabela gold é definida para calcular um agregado da versão dessa tabela do tipo 1
# MAGIC - uma view é definida por joins com tabelas definidas em outro notebook
# MAGIC   
# MAGIC ###Objetivos
# MAGIC - processar dados CDC com **`dlt.apply_changes()`**
# MAGIC - declarar live tables
# MAGIC - fazer joins com live tables
# MAGIC - descrever os notebooks da biblioteca DLT funcionam juntos em um pipeline
# MAGIC - agendamento de vários notebooks em um pipeline DLT

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingestão de dados com Auto Loader
# MAGIC Como no notebook anterior, definimos a tabela bronze em uma fonte de dados configurada com o Auto Loader.
# MAGIC 
# MAGIC Observe que o código a seguir omite a option Auto Loader para inferir o schema. Quando os dados são ingeridos de JSON sem um schema fornecido ou inferido, os campos terão os nomes corretos, mas serão todos armazenados como tipo STRING.
# MAGIC 
# MAGIC ###Especificando o nome da tabela
# MAGIC O código abaixo demosntra o uso da option **`name`** para declarar a tabela DLT. A option permite desenvolvedores especificar o nome da tabela resultante separada da definição da função que cria o DataFrame a partir do qual a tabela é definida.
# MAGIC 
# MAGIC No exemplo a seguir, usamos esta option para atender a uma convenção de nomenclatura de tabela **`<dataset-name>_<data-quality> `** e uma convenção de nomenclatura de função que descreve o que a função está fazendo. (Se não tivéssemos especificado esta option, o nome da tabela teria sido inferido da função como **`ingest_customers_cdc`**)

# COMMAND ----------

@dlt.table(
    name = "customers_bronze",
    comment = "Raw data from customers CDC feed"
)
def ingest_customers_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}/customers")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Qualidade dos Dados
# MAGIC A consulta a seguir demonstra:
# MAGIC - as 3 options para comportamento quando as constraints são violadas
# MAGIC - uma consulta com várias constraints
# MAGIC - várias condições fornecidas para uma constraint
# MAGIC - usando uma função SQL integrada em uma constraint
# MAGIC 
# MAGIC Sobre a fonte de dados:
# MAGIC - os dados são uma fonte de alimentação do CDC que contém as operações **`INSERT`**, **`UPDATE`**, e **`DELETE`** 
# MAGIC - as operações update e insert devem conter entradas válidas para todos os campos
# MAGIC - a operação delete deve conter valores **`NULL`** para todos os campos, exceto timestamp, custome_id e campos de operação.
# MAGIC 
# MAGIC Para garantir que apenas dados bons cheguem à nossa tabela silver, iremos escrever uma série de regras de imposição de qualidade que ignoram os valores nulos esperados nas operações de exclusão.
# MAGIC 
# MAGIC Vamos detalhar cada uma dessas restrições abaixo:
# MAGIC 
# MAGIC ##### **`valid_id`**
# MAGIC Essa constraint irá fazer com que nossa transação falhe se um registro conter um valor null no campo **`customer_id`**
# MAGIC 
# MAGIC ##### **`valid_operation`**
# MAGIC Essa constraint irá eliminar quaisquer registros que contenham um valor null no campo **`operation`**
# MAGIC 
# MAGIC ##### **`valid_address`**
# MAGIC Essa constraint verifica se o campo **`operation`** é **`DELETE`**. Se não for, ele verifica valores nulos em qualquer um dis 4 campos que compõem um endereço. Como não há instruções adicionais sobre o que fazer com registros inválidos, as linhas com violação serão registradas nas métricas, mas não descartadas.
# MAGIC 
# MAGIC ##### **`valid_email`**
# MAGIC Essa constraint usa o padrão regex para verificar se o valor no campo **`email`** é um endereço de email válido. Ela contém lógica para não aplicar isso a registros se o campo **`operation`** for **`DELETE`** (porque estes terão um valor nulo para o campo **`email`**). Registros que violam são descartados.

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IS NOT NULL")
@dlt.expect("valid_name", "name IS NOT NULL or operation = 'DELETE'")
@dlt.expect("valid_adress", """
    (address IS NOT NULL and 
    city IS NOT NULL and 
    state IS NOT NULL and 
    zip_code IS NOT NULL) or
    operation = "DELETE"
    """)
@dlt.expect_or_drop("valid_email", """
    rlike(email, '^([a-zA-Z0-9_\\\\-\\\\.]+)@([a-zA-Z0-9_\\\\-\\\\.]+)\\\\.([a-zA-Z]{2,5})$') or 
    operation = "DELETE"
    """)
def customers_bronze_clean():
    return (
        dlt.read_stream("customers_bronze")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Processando Dados CDC com **`dlt.apply_changes()`**
# MAGIC 
# MAGIC DLT apresenta uma nova estrutura sintática para simplificar o processamento CDC.
# MAGIC 
# MAGIC **`dlt.apply_changes()`** tem as seguintes garantias e requisitos:
# MAGIC - executa a ingestão incremental/streaming de dados CDC
# MAGIC - fornece sintaxe simples para simplificar um ou mais campos com chave primária para uma tabela
# MAGIC - a suposição padrão é que as linhas conterão inserções e atualizações
# MAGIC - pode opcionalmente aplicar exclusões
# MAGIC - ordena automaticamente os registros atrasados usando a cheve de sequenciamento fornecida pelo usuário
# MAGIC - usa uma sintaxe simples para especificar colunas a serem ignoradas com **`except_column_list`**
# MAGIC - o padrão será aplicar alterações como SCD Tipo 1
# MAGIC 
# MAGIC O código a seguir:
# MAGIC * cria a tabela **`customers_silver`**; **`dlt.apply_changes()`** requer que a tabela destino seja declarada em uma instrução separada
# MAGIC * identifica a tabela **`customers_silver`** como o destino no qual as alterações serão aplicadas
# MAGIC * especifica a tabela **`customers_bronze_clean`** como a fonte de streaming
# MAGIC * identifica o campo **`customer_id`** como chave primária
# MAGIC * especifica que os registros onde o campo **`operation`** for **`DELETE`** deve ser aplicado como exclusão
# MAGIC * especifica o campo **`timestamp`** para ordenar como as operações devem ser aplicadas
# MAGIC * indica que os campos devem ser adicionados à tabela de destino, exceto **`operation`**, **`source_file`**, e **`_rescued_data`**

# COMMAND ----------

dlt.create_target_table(
    name = "customers_silver")

dlt.apply_changes(
    target = "customers_silver",
    source = "customers_bronze_clean",
    keys = ["customer_id"],
    sequence_by = F.col("timestamp"),
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    except_column_list = ["operation", "source_file", "_rescued_data"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Consultando tabelas com alterações aplicadas
# MAGIC O padrão **`dlt.apply_changes()`** é criar uma tabela SCD tipo 1, o que significa que cada chave exclusiva terá no máximo 1 registro e que as atualizações substituirão as informações originais.
# MAGIC 
# MAGIC Embora o destino de nossa operação na célula anterior tenha sido definido como uma streaming live table, os dados estão sendo atualizados e excluídos nesta tabela (e, portanto, quebra os requisitos somente de anexação para fontes de streaming live table). Dessa forma, as operações downstream não podem realizar consultas de streaming nessa tabela.
# MAGIC 
# MAGIC Esse padrão garante que, se alguma atualização chegar fora da ordem, os resultados downstream possam ser reclaculados adequadamente para refletir as atualizações. Ele também garante que, quando os registros são excluídos de uma tabela de origem, esse valores não sejam mais refletidos nas tabelas posteriormente no pipeline.
# MAGIC 
# MAGIC Abaixo, definimos uma consulta agragada simples para criar uma tabela dinâmica a partir dos dados da tabela **`customers_silver`**.

# COMMAND ----------

@dlt.table(
    comment="Total active customers per state")
def customer_counts_state():
    return (
        dlt.read("customers_silver")
            .groupBy("state")
            .agg( 
                F.count("*").alias("customer_count"), 
                F.first(F.current_timestamp()).alias("updated_at")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Views DLT
# MAGIC A consulta abaixo define uma view DLT usando o decorador **`@dlt.view`**.
# MAGIC 
# MAGIC As views em DLT diferem das tabelas persistentes e também podem herdar a execução de straming da função que decoram.
# MAGIC 
# MAGIC As views têm a mesmas garantias de atualização que as tabelas ativas, mas os resultados das consultas não são armazenados em disco.
# MAGIC 
# MAGIC Ao contrário das views usadas em outros lugares no Databricks, as Views DLT são não persistidas no metastores, o que significa que elas só podem ser referenciadas de dentro do pipeline DLT do qual fazem parte (Este é um escopo semelhante ao DataFrame em notebooks databricks).
# MAGIC 
# MAGIC As views ainda podem ser usadas para impor a qualidade dos dados, e as métricas das exibições serão coletadas e relatadas como seriam para as tabelas.
# MAGIC 
# MAGIC ###Joins e tabelas de referência em bibliotecas de notebooks
# MAGIC O código que analisamos até agora mostrou 2 conjuntos de dados de origem se propagando por uma série de etapas em notebooks separados.
# MAGIC 
# MAGIC O DLT oferece suporte ao agendamento de vários notebooks como parte de uma única configuração de DLT pipeline. Você pode editar pipelines DLT existentes para adicionar notebooks adicionais.
# MAGIC 
# MAGIC Dentro de um DLT pipeline, o código em qualquer biblioteca de notebook pode fazer referência a tabelas e views criadas em qualquer outra biblioteca de notebook.
# MAGIC 
# MAGIC Essencilamente, podemos pensar no escopo da referência do banco de dados pela palavra cheva **`LIVE`** para estar no nível do DLT pipeline, em vez do notebook individual.
# MAGIC 
# MAGIC Na consulta abaixo, criamos uma nova view juntando as tabelas silver de nossos conjunto de dados **`orders`** e **`customers`**. Observe que essa view não é definida como streaming. Como tal, sempre iremos capturar o **`email`** atual válido para cada **`customers`** e iremos descartar automaticamente os registros dos clientes depois que eles forem exclupidos da tabela **`customers_silver`**.

# COMMAND ----------

@dlt.view
def subscribed_order_emails_v():
    return (
        dlt.read("orders_silver").filter("notifications = 'Y'").alias("a")
            .join(
                dlt.read("customers_silver").alias("b"), 
                on="customer_id"
            ).select(
                "a.customer_id", 
                "a.order_id", 
                "b.email"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Adicionando este notebook a pipeline DLT
# MAGIC A adição de bibliotecas de notebook adicionais a um pipeline existente é realizada facilmente com a interface do usuário DLT.
# MAGIC 1. Navegue até o pipeline DLT que você configurou anteriormente no curso
# MAGIC 1. Clique no botão **Settings** no topo direito da tela
# MAGIC 1. Em **Notebook Libraries**, clique **Add notebook library**
# MAGIC    * Use o seletor de \rquivos para selecionar este notebook, em seguida clique em **Select**
# MAGIC 1. Clique no botão **Save** para salver suas atualizações
# MAGIC 1. Clique no botão azul **Start** no canto superior direito da tela para atualizar seu pipeline e processar novos registros
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> O link para o notebook pode ser encontrado em [4.1 - DLT UI Walkthrough]($../4.1 - DLT UI Walkthrough)<br/>
