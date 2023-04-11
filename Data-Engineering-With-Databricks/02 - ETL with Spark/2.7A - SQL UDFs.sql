-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SQL UDFs (Funções Definidas pelo Usuário)
-- MAGIC ###Objetivos:
-- MAGIC - Definir e registrar SQL UDFs
-- MAGIC - Usar declarações **`CASE`** / **`WHEN`** em SQL
-- MAGIC - Aproveitar declarações **`CASE`** / **`WHEN`** em SQL para um fluxo de controle personalisado

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.7A

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###UDFs
-- MAGIC As funções definidas pelo usuário (UDFs) em Spark SQL permitem que você registre uma lógica SQL personalizada como functions em um banco de dados, tornando este método reutilizável em qualquer lugar onde o SQL possa ser executado no Databricks. Essas funções são registradas nativamente no SQL e matem todas as otimizações do Spark ao aplicar a lógica personalizada a grandes conjuntos de dados.
-- MAGIC 
-- MAGIC No mínimo, a criação de um SQL UDF requer um nome para a função, parâmetros opcionais, o tipo a ser retornado e alguma lógica personalizada.
-- MAGIC 
-- MAGIC A seguir, uma amostra de uma function chamada **`sale_announcement`** que tem **`item_name`** e **`item_price`** como parâmetros. Ela retorna uma string que anuncia a venda de um item a 80% de seu preço original.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name, price) AS message FROM item_lookup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que esta função é aplicada para todos os valores da coluna de maneira paralela no mecanismo de processamento do Spark. SQL UDFs são uma forma efetiva de definir uma lógica personalizada que é otimizada para execução em Databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Escopo e permissões das SQL UDFs
-- MAGIC SQL UDFs:
-- MAGIC - Persiste entre o ambiente de execução (que pode incluir notebooks, DBSQL queries e jobs)
-- MAGIC - Existe como objetos no metastore e são governadas pelas mesmas ACLs como tabelas, databases ou views
-- MAGIC - Exige permissão **`USAGE`** e **`SELECT`** para usar a SQL UDF
-- MAGIC 
-- MAGIC Podemos usar **`DESCRIBE FUNCTION`** para vê onde uma function foi registrada e informações básicas sobre entradas esperadas e o que é retornado (e ainda mais informações com **`DESCRIBE FUNCTION EXTENDED`**) 

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que o campo **`Body`** na parte inferior da descrição da função mostra a lógica SQL usada na própria função.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Funções de fluxo de controle simples
-- MAGIC Combinando SQL UDFs com fluxo de controle na forma de cláusulas **`CASE`** / **`WHEN`** fornece execução otimizada para fluxos de controle em cargas de trabalho SQL. O padrão SQL de construção sintática **`CASE`** / **`WHEN`** permite a avaliação de várias declarações condicionais com resultados alternativos com base no conteúdo da tabela.
-- MAGIC 
-- MAGIC Aqui, demonstramos o envolvimento dessa lógica de fluxo de controle em uma função que será reutilizável em qualquer lugar em que possamos executar o SQL.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE 
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
  ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name, price) FROM item_lookup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Embora os exemplos fornecidos aqui sejam simples, estes mesmo princípios básicos podem ser usados para adicionar cálculos personalizados e lógicas para execução nativa no Spark SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
