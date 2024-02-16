-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Sintaxe Fundamental do DLT SQL
-- MAGIC
-- MAGIC Este notebook demostra o uso de Delta Live Table (DLT) para processar dados brutos de arquivos JSON que chegam ao armazenamento de objetos na nuvem por meio de uma série de tabelas para direcionar cargas de trabalho analíticas no Lakehouse. Aqui, vamos demostrar a arquitetura medalhão, no qual os dados são transformados e enriquecidos incrementalmente à medida que fluem por um pipeline. Este notebook foca na sintaxe SQL do DLT, em vez dessa arquitetura, mas uma breve visão geral:
-- MAGIC - A tabela bronze contém os registros brutos carregados do JSON enriquecidos com dados que descrevem como os registros foram ingeridos
-- MAGIC - A tabela silver valida e enriquece os campos de interesse
-- MAGIC - A tabela gold contém dados agregados que direcionam insights de negócios e dashboards
-- MAGIC
-- MAGIC ###Objetivos:
-- MAGIC - Declara Delta Live Tables
-- MAGIC - Ingerir dados com Auto Loader
-- MAGIC - Usar parâmetros nos pipelines DLT
-- MAGIC - Forçar qualidade nos dados com constraints
-- MAGIC - Adicionar comentários nas tabelas
-- MAGIC - Descrever diferenças na sintaxe e execução de live tables e streaming live tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Sobre a biblioteca DLT
-- MAGIC **A sintaxe DLT não é destinada à execução interativa em um notebook.** Este notebook precisará ser agendado como parte do pipeline DLT para execução adequada.
-- MAGIC
-- MAGIC Se você executar uma célula do notebook DLT interativamente, você deverá ver uma mensagem que sua instrução está sintaticamente. Observe que embora algumas verificações de sintaxe são executadas antes de retornar esta mensagem, isso não é garantia que sua consulta será executada conforme desejado. Nós iremos discutir o desenvolvimento e a solução do código DLT posteriomente.
-- MAGIC
-- MAGIC ### Parametrização
-- MAGIC Durante a configuração do DTL pipeline, um número de options foram especificadas. Uma dessas foi um par chave-valor adicionado para o campo **Configurations**.
-- MAGIC
-- MAGIC Configurações em pipelines DLT são similares para parâmetros no Databricks Jobs ou widgets nos notebooks Databricks.
-- MAGIC
-- MAGIC Ao longo dessa lição usaremos **`${source}`** para executar a substituição de string da path do caminho definida durante a configuração em nossas consultas SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Tabelas como resultado de consultas
-- MAGIC Delta Live Tables adapta consultas SQL padrão para combinar DDL(linguagem de definição de dados) e DML(linguagem de manipulação de dados) em uma sintaxe declarativa unificada.
-- MAGIC
-- MAGIC Existem dois tipos distintos de tabelas persistidas que podem ser criadas com DLT:
-- MAGIC * **Live tables** são views materializadas para o Lakehouse. Elas irão retornar os resultados atuais de qualquer consulta a cada atualização
-- MAGIC * **Streaming live tables** são projetadas para processamento de dados incremental, quase em tempo real
-- MAGIC
-- MAGIC Observe que ambos objetos são persistidos como tabelas armazenadas com o protocolo Delta Lake (fornecendo transações ACID, versionamento e muitos outros benefícios). Iremos falar mais sobre a diferença entre live tables e streaming live tables depois.
-- MAGIC
-- MAGIC Para ambos os tipos de tabelas, DLT adota a abordagem de uma instrução CTAS(create table as select) levemente modificada. Os engenheiros só precisam se preocupar em escrever consultas para transformar seus dados e DLT cuida do resto.
-- MAGIC
-- MAGIC A sintaxe básica para uma consulta SQL DLT é:
-- MAGIC
-- MAGIC **`CREATE OR REFRESH [STREAMING] LIVE TABLE table_name`**<br/>
-- MAGIC **`AS select_statement`**<br/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Ingestão Streaming com Auto Loader
-- MAGIC A Databricks desenvolveu a funcionalidade Auto Loader para fornecer execução otimizada para carregar dados de forma incremental do armazenamento de objetos em nuvem para o Delta Lake. Usando Auto Loader com DLT é simples: basta configurar um diretório de dados de origem, fornecer algumas definições de configurações e escrever a consulta nos dados de origem. Auto Loader irá automaticamente detectar novos arquivos de dados à medida que eles chegarem ao local de armazenamento de objetos na nuvem de origem, processando de forma incremental novos registros sem a necessidade de executar varreduras caras e recalcular resultados para conjuntos de dados em crescimento infinito.
-- MAGIC
-- MAGIC O método **`cloud_files()`** permite que o Auto Loader seja usado nativamente com o SQL. Este método usa os seguintes parâmetros:
-- MAGIC - o local de origem, que deve ser o armazenamento de objetos baseado em nuvem
-- MAGIC - o formato dos dados de origem, que neste caso é JSON
-- MAGIC - uma lista separada por vírgulas de tamanho arbitrário de opções do leitor. Neste caso, definimos **`cloudFiles.inferColumnTypes`** para **`true`**
-- MAGIC
-- MAGIC A consulta a seguir, além dos campos contidos na fonte, a função Spark SQL para **`current_timestamp()`** e **`input_file_name()`** são usadas para capturar informações sobre quando o registro foi ingerido e a fonte de arquivos específica para cada registro.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Validando, enriquecendo e transformando os dados
-- MAGIC DLT permite que usuários declarem facilmente tabelas a partir de resultados de qualquer transformação Spark padrão. DLT aproveita recursos usados  em outros lugares no Spark SQL para documentar conjuntos de dados, enquanto adiciona novas funcionalidades para verificar a qualidade dos dados.
-- MAGIC
-- MAGIC Vamos detalhar a sintaxe da consulta abaixo.
-- MAGIC
-- MAGIC ### A Instrução Select
-- MAGIC A instrução **select** contém a lógica principal da sua consulta. Neste exemplo nós:
-- MAGIC - convertemos o campo **`order_timestamp`** para o tipo timestamp
-- MAGIC - selecionamos todos os campos restantes (exceto uma lista de 3 nos quais não estamos interessados, incluindo o original **`order_timestamp`**)
-- MAGIC
-- MAGIC Observe que a cláusula **`FROM`** tem duas construções que podem não ser familiares para você:
-- MAGIC - A chave **`LIVE`** é usada no lugar do nome da base de dados para referir ao banco de dados destino configurado para o atual pipeline DLT
-- MAGIC - O método **`STREAM`** permite que os usuários declarem uma fonte de dados de streaming para consultas SQL
-- MAGIC
-- MAGIC Observe que se o banco de dados destino não é declarado durante a configuração do pipeline, suas tabelas não serão publicadas (ou seja, elas não serão registradas no metastore e disponibilizadas para consultas em outro lugar). O banco de dados destino pode ser facilmente alterado ao se mover entre diferentes ambientes de excução, o que significa que o mesmo código pode facilmente ser implantado em cargas de trabalho regionais ou promovido de um ambiente de desenvolvimento para produção sem a necessidade de codificar nomes de banco de dados.
-- MAGIC
-- MAGIC ### Restrições de Qualidade de Dados (Data Quality Constraints)
-- MAGIC DLT usa instruções simples booleanas para permitir verificações de imposição de qualidade nos dados. Na declaração abaixo, nós:
-- MAGIC - declaramos uma constraint chamada **`valid_date`**
-- MAGIC - definimos a verificação condicional que o campo **`order_timestamp`** deve conter: um valor maior que 01 de janeiro que 2021.
-- MAGIC - instruímos o DLT a falhar na transação atual se algum registro violar a restrição
-- MAGIC
-- MAGIC Cada constraint pode ter várias condições e várias constraints podem ser definidas para uma única tabela. Além de falhar na atualização, a violação de uma constraint também pode descartar registros automaticamente ou apenas registrar o número de violações enquanto ainda processa esses registros inválidos.
-- MAGIC
-- MAGIC ###Comentários nas tabelas
-- MAGIC Os comentários nas tabelas são um padrão em SQL e podem ser usados para fornecer informações úteis para usuários. Neste exemplo, escrevemos uma breve descrição legível da tabela que descreve como os dados estão sendo ingeridos e aplicados (o que também pode ser obtido da revisão de outros metadados da tabela).
-- MAGIC
-- MAGIC ###Propriedades da tabela
-- MAGIC O campo **`TBLPROPERTIES`** pode ser usado para passar qualquer número de pares de chave/valor para marcação personalizada de dados. Aqui, definimos o valor silver para a chave quality.
-- MAGIC
-- MAGIC Observe que embora estes campos permitam que as tags personalizadas sejam definidas arbitrariamente, ele também é usado para definir várias configurações que controlam o desempenho de uma tabela. Ao revisar os detalhes da tabela, você também pode encontrar várias configurações que são ativadas por padrão sempre que uma tabela é criada.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_silver
(CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
COMMENT "Append only orders with valid timestamps"
TBLPROPERTIES ("quality" = "silver")
AS SELECT timestamp(order_timestamp) AS order_timestamp, * EXCEPT (order_timestamp, source_file, _rescued_data)
FROM STREAM(LIVE.orders_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Live Tables vs Streaming Live Tables
-- MAGIC As duas consultas que analisamos até agora criaram streaming live tables. Abaixo, vemos uma consulta simples que retorna uma live table (ou view materializada) de alguns dados agregados.
-- MAGIC
-- MAGIC O Spark diferenciou historicamente entre consultas batch e consultas streaming. Lives table e streaming live tables têm semelhanças.
-- MAGIC
-- MAGIC Observe que as únicas diferenças sintáticas entre streaming live tables e live tables são a falta da palavra chave **`STREAMING`** na cláusula create e não agrupar a tabela de origem no método **`STREAM()`**.
-- MAGIC
-- MAGIC Abaixo estão algumas das diferenças entre esses tipos de tabelas.
-- MAGIC
-- MAGIC ###Live tables
-- MAGIC - sempre "correta", o que significa que seu conteúdo corresponderá à sua definição após qualquer atualização
-- MAGIC - retorna os mesmos resultados como se a tabela tivesse acabado de ser definida pela primeira vez em todos os dados
-- MAGIC - não deve ser modificada por operações externas ao pipeline DLT (você obterá respostas indefinidas ou sua alteração será desfeita)
-- MAGIC
-- MAGIC ###Streaming Live Tables
-- MAGIC - oferece suporte apenas à leitura de fontes de streaming "somente anexadas"
-- MAGIC - lê cada lote de entrada apenas uma vez, não importa o que aconteça (mesmo se as dimensões unidas mudarem ou se a definição da consulta mudar, etc)
-- MAGIC - pode executar operações na tabela fora do pipeline DLT gerenciado (acrescentar dados, executar GDPR, etc)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE orders_by_date
AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
FROM LIVE.orders_silver
GROUP BY date(order_timestamp)
