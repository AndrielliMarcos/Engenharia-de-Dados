-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Sintaxe DLT SQL para resolução de problemas
-- MAGIC Agora que passamos pelo processo de configuração e execução de um pipeline com 2 notebooks, iremos simular o desemvolvimento e adição de um terceiro notebook.
-- MAGIC 
-- MAGIC O código fornecido a seguir contem alguns pequenos erros de sintaxe intencional. Ao solucionar esses erros, iremos aprender como desenvolver código DLT iterativamente e identificar erros em sua sintaxe.
-- MAGIC 
-- MAGIC Esta lição não pretende fornecer uma solução robusta para desenvolvimento e teste de código, em vez disso, destina-se ajudar os usuários a começar com DLT e lidar uma sintaxe desconhecida.
-- MAGIC 
-- MAGIC ###Objetivos:
-- MAGIC - Identificar e solucionar problemas de sintaxe DLT
-- MAGIC - Desenvolver iterativamente pipelines DLT com notebooks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Adicionar este notebook a uma pipeline DLT
-- MAGIC Neste ponto do curso, você deve ter um pipeline configurado com 2 bibliotecas de notebook.
-- MAGIC 
-- MAGIC Você deve ter processado vários lotes de registros por meio desse pipeline e deve entender como acionar uma nova execução do pipeline e adicionar uma biblioteca adicional.
-- MAGIC 
-- MAGIC Para começar este lição, siga o processo de adicionar es te notebook ao seu pipeline usando a interface do usuário DLT e acione uma atualização.
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> O link para este notebook pode ser encontrado em [4.1 - DLT UI Walkthrough]($../4.1 - DLT UI Walkthrough), na sessão **Gerar configuração do pipeline**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Solucionar erros
-- MAGIC Cada uma das três consultas a seguir contém um erro de sintaxe, mas cada um desses erros serão detectados e reportados de maneira ligeiramente diferente pelo DLT.
-- MAGIC 
-- MAGIC Algumas sintaxes de erro serão detectados durante o estágio de **Initializing**, pois a DLT não é capaz de analisar corretamente os comando.
-- MAGIC 
-- MAGIC Outros erros de sintaxe serão detectados durante o estágio de **Setting up tables**.
-- MAGIC 
-- MAGIC Observe que devido a forma que a DLT resolve a ordem das tabelas no pipeline em diferentes etapas, à vezes você pode ver erros lançados para estágios posteriores primeiro.
-- MAGIC 
-- MAGIC Uma abordagem que funciona bem é corrigir uma tabela por vez, começando no seu conjunto de dados mais antigo e trabalhando até chegar ao final. O código comentado será ignorado automaticamente, para que você possa remover com segurança o código de uma execução de desenvolvimento sem removê-lo completamente.
-- MAGIC 
-- MAGIC Mesmo que você consiga detectar imediatamente os erros no código abaixo, tente usar as mensagens de erro da interface do usuário para orientar a identificação desses erros. O código da solução segue na célula abaixo.

-- COMMAND ----------

-- TODO
CREATE OR REFRESH STREAMING LIVE TABLE status_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/status", "json");

CREATE OR REFRESH STREAMING LIVE TABLE status_silver
(CONSTRAINT valid_timestamp EXPECT (status_timestamp > 1640995200) ON VIOLATION DROP ROW)
AS SELECT * EXCEPT (source_file, _rescued_data)
FROM LIVE.status_bronze;

CREATE OR REFRESH LIVE TABLE email_updates
AS SELECT a.*, b.email
FROM status_silver a
INNER JOIN LIVE.subscribed_order_emails_v b
ON a.order_id = b.order_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Soluções
-- MAGIC A sintaxe correta para cada uma de nossas funções acima é fornecida na célula a seguir.

-- COMMAND ----------

-- Resposta
CREATE OR REFRESH STREAMING LIVE TABLE status_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/status", "json");

CREATE OR REFRESH STREAMING LIVE TABLE status_silver
(CONSTRAINT valid_timestamp EXPECT (status_timestamp > 1640995200) ON VIOLATION DROP ROW)
AS SELECT * EXCEPT (source_file, _rescued_data)
FROM STREAM(LIVE.status_bronze);

CREATE OR REFRESH LIVE TABLE email_updates
AS SELECT a.*, b.email
FROM LIVE.status_silver a
INNER JOIN LIVE.subscribed_order_emails_v b
ON a.order_id = b.order_id;

-- COMMAND ----------


