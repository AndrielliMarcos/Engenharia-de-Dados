-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Configurando Tabelas Delta
-- MAGIC Depois de extrair os dados da fontes de dados externas, carregue os dados no Lakehouse para garantir que todos os benefícios do Databricks possam ser totalmente aproveitados.
-- MAGIC 
-- MAGIC Embora diferentes organizações possam ter políticas variadas para como os dados são carregados inicialmente no Databricks, normalmente recomendamos que as primeiras tabelas representem uma versão basicamente bruta dos dados e que a validação e o enriquecimento ocorram em estágios posteriores. Este padrão garante que mesmo que os dados não correspondam às expectativas em relação aos tipos de dados ou nomes de colunas, nenhum dado será excluído, o que significa que intervenção programática ou manual ainda pode salvar o dado em um estado parcialmente corrompido ou inválido.
-- MAGIC 
-- MAGIC Esta lição irá focar principalmente no padrão usado para criar a maioria das tabelas: **`CREATE TABLE _ AS SELECT`** (CTAS)
-- MAGIC 
-- MAGIC ###Objetivos:
-- MAGIC - Usar declarações CTAS para criar tabelas Delta Lake
-- MAGIC - Criar novas tabelas de views ou tabelas existentes
-- MAGIC - Enriquecer os dados carregado com metadados adicionais
-- MAGIC - Declarar schema de tabela com colunas geradas e comentários descritivos
-- MAGIC - Definir opções avançadas para controlar a localização dos dados, aplicação de qualidade e particionamento

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-03.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Table as Select (CTAS)
-- MAGIC As instruções **`CREATE TABLE AS SELECT`** criam e populam as tabelas Delta usando dados recuperados de uma consulta de entrada.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As instruções CTAS inferem as informações schema automaticamente do resultado da consulta e não oferecem suporte à declaração manual do esquema. 
-- MAGIC 
-- MAGIC Isso significa que as instruções CTAS são úteis para ingestão de dados externos de fontes com schema bem definido, como arquivos e tabelas Parquet.
-- MAGIC 
-- MAGIC As instruções CTAS também não oferecem suporte à especificação de opções de arquivo adicionais.
-- MAGIC 
-- MAGIC Podemos ver como isso apresentaria limitações significativas ao tentar ingerir dados de arquivos CSV.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Para ingestão correta destes dados para uma tabela Delta Lake, precisaremos usar uma referência aos arquivos que nos permita especificar opções.
-- MAGIC 
-- MAGIC Na lição anterior, nós mostramos como fazer isso registrando uma tabela externa. Aqui, desenvolveremos ligeiramente essa sintaxe para especificar as opções para uma temp view  e, em seguida, usaremos essa temp view como a origem de uma istrução STAS para registrar com êxito a tabela Delta

-- COMMAND ----------

-- cria a temp view
CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

-- cria a tabela a partir da temp view
CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Filtrando e renomeando colunas de tabelas existentes
-- MAGIC Transformações simples como alterar o nome de coluna ou omitir colunas da tabela de destino podem ser facilmente realizadas durante a criação da tabela.
-- MAGIC 
-- MAGIC A istrução a seguir cria uma nova tabela contendo um subconjunto de colunas da tabela **`sale`**.
-- MAGIC 
-- MAGIC Aqui, iremos presumir que estamos intencionalmente deixando de fora informações que potencialmente identificam o usuário ou que fornecem detalhes da compra. Também renomearemos nossos campos com a suposição de que um sistema downstream tem convenções de nomenclatura diferentes de nossos dados de origem.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Observe que poderíamos ter alcançado esse mesmo objetivo com uma view, conforme mostrado abaixo.

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Declarar schema com colunas geradas
-- MAGIC Como notado anteriormente, as instruções CTAS não suportam declaração de schema. Observamos acima que a coluna timestamp parece ser alguma variante da Unix timestamp, que pode não ser o mais útil para nossos analistas obterem insights. Esta é uma situação onde colunas geradas seriam benéficas.
-- MAGIC 
-- MAGIC As colunas geradas são um tipo especial de colunas nos quais os valores são gerados automaticamente baseado em uma função especificada pelo usuário sobre outras colunas na tabela Delta.
-- MAGIC 
-- MAGIC O código abaixo demostra a criação de uma nova tabela:
-- MAGIC 1. Especificando o nome e o tipo da coluna
-- MAGIC 1. Adicionando uma coluna gerada para calcular a data
-- MAGIC 1. Fornecendo uma coluna descritiva de comentário para a coluna gerada

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como **`date`** é uma coluna gerada, se escrevermos em **`purchase_dates`** sem fornecer varlores para a coluna **`date`**, o Delta Lake os calculará automaticamente.
-- MAGIC 
-- MAGIC **Observação:** as células a seguir define uma configuração para permitir a geração de colunas usando a instrução Delta Lake **`MERGE`**.

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Podemos vê a seguir que todas as datas foram computadas corretamente conforme os dados foram inseridos, embora nem nossos dados de origem nem a consulta de inserção tenham especificado os valores neste campo.
-- MAGIC 
-- MAGIC Como acontece com qualquer fonte Delta Lake, a consulta lê automaticamente os dados mais recentes da tabela para qualquer consulta, nunca é preciso executar **`REFRESH TABLE`**

-- COMMAND ----------

REFRESH TABLE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC É importante observar que se um campo que seria gerado for incluído em uma inserção de uma tabela, essa inserção falhará se o valor fornecido não corresponder exatamente ao valor que seria derivado pela lógica usada para definir a coluna gerada.
-- MAGIC 
-- MAGIC Vamos ver este erro ao descomentar e executar a célula abaixo:

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Adicionando uma restrição (constraint) em uma tabela
-- MAGIC A mensagem de erro acima refere-se a um **`CHECK constraint`**. Colunas geradas são uma implementação especial de restrições de verificação.
-- MAGIC 
-- MAGIC Como o Delta Lake impõe o schema na gravação, o Databricks pode oferecer suporte a cláusulas de gerenciamento de retrição SQL padrão para garantir a qualidade e a integridade dos dados adicionados a uma tabela.
-- MAGIC 
-- MAGIC O Databricks suporta atualmente 2 tipos de restrição:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** constraints</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** constraints</a>
-- MAGIC 
-- MAGIC Nos dois casos, você deve assegurar que nenhum dado que viole a restrição já esteja na tabela antes de definir a restrição. Uma vez que a restrição tenha sido adicionada a tabela, os dados que violarem a restrição resultarão em falha de gravação.
-- MAGIC 
-- MAGIC A seguir, iremos adicionar a restrição **`CHECK`** para a coluna **`date`** da nossa tabela. Observe que a restrição **`CHECK`** parecem cláusulas **`WHERE`** padrão que você pode usar para filtrar um conjunto de dados.

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Restrições de tabela são mostradas no campo **`TBLPROPERTIES`**.

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Enriqueça tabela com opções e metadados adicionais
-- MAGIC Até agora, apenas arranhamos a superfície no que diz respeito às opções para enriquecer as tabelas Delta.
-- MAGIC 
-- MAGIC A seguir, mostramos a evolução de uma istrução CTAS para incluir várias configurações e metadados adicionais.
-- MAGIC 
-- MAGIC Nossa cláusula **`SELECT`** utiliza dois comando integrados do Spark SQL úteis para ingestão de arquivos:
-- MAGIC * **`current_timestamp()`** registra o timestamp quando a lógiva é executada
-- MAGIC * **`input_file_name()`** registra o arquivo de dados de origem para cada registro na tabela
-- MAGIC 
-- MAGIC Também incluímos uma lógica para criar uma nova coluna de data derivada da fonte de dados timestamp.
-- MAGIC 
-- MAGIC A cláusula **`CREATE TABLE`** contém várias opções:
-- MAGIC - um **`COMMENT`** é adicionado para permitir uma descoberta mais fácil do conteúdo da tabela
-- MAGIC - um **`LOCATION`** é especificado, que irá resultar em uma tabela externa (ao invés de uma tabela gerenciada)
-- MAGIC - a tabela é particionada por uma coluna de data usando a instrução **`PARTITIONED BY`**. Isso significa que os dados existirão em seu próprio diretório no local de armazenamento destino.
-- MAGIC 
-- MAGIC **Observação:** o particionamento é mostrado aqui principalmente para demonstrar a sintaxe e o impacto. A maioria das tabelas Delta Lake (especialmente dados de tamanho pequeno a médio) não irão se beneficiar do particionamento. Como o particionamento separa fisicamente os arquivos de dados, essa abordagem pode resultar em um problema de arquivos pequenos e impedir a compactação de arquivos e o salto eficiente de dados. Os benefícios observados no Hive ou HDFS não se traduzem em Delta Lake, e você deve consultar um arquiteto de Delta Lake esperiente antes de particionar tabelas.
-- MAGIC 
-- MAGIC **Como prática recomendada, você deve usar tabelas não particionadas como padrão para a maioria dos casos de uso ao trabalhar com o Delta Lake.**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Os campos de metadados adicionados na tabela fornecem informação útil para entender quando os registros são inseridos e de onde eles são. Isto pode ser especialmente útil se for necessário solucionar problemas nos dados de origem.
-- MAGIC 
-- MAGIC Todos os comentários e propriedades de uma determinada tabela podem ser revisados usando **`DESCRIBE TABLE EXTENDED`**.
-- MAGIC 
-- MAGIC **Observação:** O Delta Lake adiciona automaticamente várias propriedades de tabela na criação da tabela.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Listar o local usado para a tabela revela que os valores exclusivos na coluna de partição **`first_touch_date`** são usados para criar diretórios de dados.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Clonando tabelas Delta Lake
-- MAGIC O Delta Lake tem duas opções para copiar tabelas do Delta Lake com eficiência.
-- MAGIC 
-- MAGIC **`DEEP CLONE`** copia completamente os dados e metadados da fonte de origem para a tabela destino. Esta cópia ocorre incrementalmente, portanto, executando este comando novamente pode sincronizar mudanças da origem para o local de destino.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Como todos os arquivos de dados devem ser copiados, isso pode demorar um pouco para grandes conjuntos de dados.
-- MAGIC 
-- MAGIC Se você deseja criar uma cópia de uma tabela rapidamente para testar a aplicação de alterações sem o risco de modificar a tabela atual, **`SHALLOW CLONE`** pode ser uma boa opção. **`SHALLOW CLONE`** copiam apenas os logs de transação Delta, o que significa que os dados não se movem.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Em ambos os casos, as modificações de dados aplivadas à versão clonada da tabela serão rastreadas e armazenadas separadamente da origem. A clonagem é uma ótima maneira de configurar tabelas para testar o código SQL ainda em desenvolvimento.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()
