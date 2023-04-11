-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Remodelagem de dados
-- MAGIC Neste laboratório vamos criar uma tabela chamada clickpaths que agrega o número de vezes que cada usuário realizou uma determinada ação em **`events`** e juntar essa informações com uma view **`transactions`** para criar um registro das ações e compras finais de cada usuário.
-- MAGIC 
-- MAGIC A tabela clickpaths deve conter todos os campos da view transaction, assim como uma contagem de cada event_name da tabela events.
-- MAGIC 
-- MAGIC ###Objetivos
-- MAGIC - Tabela Pivot e joins para criar o clickpaths de cada usuário

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.6L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Usaremos python para executar verificações ocasionalmente por todo o curso. A function a seguir irá retornar um erro com uma mensagem com o que é necessário alterar se você não tiver seguido as instruções. Se não tiver nenhuma saída, significa que esta etapa foi concluída

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Tabela pivot events para obter a contagem de eventos para cada usuário
-- MAGIC Vamos começar criando uma tabela pivot a partir da tabela **`events`** para obter contagens para cada **`event_name`**.
-- MAGIC 
-- MAGIC Queremos agregar o número de vezes que cada usuário executou um evento específico, especificado na coluna **`event_name`**. Para fazer isso, agrupe por **`user_id`** e faça o pivot em **`event_name`** para fornecer a contagem de todos os tipos de eventos na coluna, resultando no schema abaixo. Observe que **`user_id`** está renomeado para **`user`**.
-- MAGIC 
-- MAGIC 
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |

-- COMMAND ----------

SELECT * FROM events

-- COMMAND ----------

-- TODO
CREATE OR REPLACE VIEW events_pivot
AS SELECT * FROM (
  SELECT user_id user, event_name
  FROM events
)
PIVOT ( count(*) FOR event_name IN( "cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
"register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
"cc_info", "foam", "reviews", "original", "delivery", "premium"
))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Join
-- MAGIC Agora, junte **`events_pivot`** com **`transactions`** para cria a tabela **`clickpaths`**. Esta tabela deve ter o mesmo nome das colunas da tabela **`events_pivot`** criada acima, seguido por colunas da tabela **`transactions`**.
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMP VIEW clickpaths AS
SELECT * 
FROM events_pivot a
JOIN transactions b 
  ON a.user = b.user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
