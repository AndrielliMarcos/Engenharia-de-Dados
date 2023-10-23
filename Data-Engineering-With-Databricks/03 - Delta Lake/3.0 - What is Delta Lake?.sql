-- Databricks notebook source
-- MAGIC %md
-- MAGIC Delta Lake é um projeto open-source que permite construir um data lakehouse sobre os sistemas de armazenamento já existentes, como um data lake.
-- MAGIC - O Delta Lake se baseia em formatos padrão, como Parquet
-- MAGIC - É otimizado para armazenamento de objetos em nuvem
-- MAGIC - Construído para manipulação de metadados escaláveis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC O Delta Lake é alimentado principalmente por dados armazenados no formato parquet, um dos mais populares formatos open-source para trabalhar com big data. O parquet também oferece alavancagem de metadados adicional sobre outros formatos de código aberto, como JSON.
-- MAGIC
-- MAGIC Ele é otimizado para armazenamento de objetos em nuvem. Embora o Delta Lake possa ser executado em vários meios de armazenamento diferentes, ele foi otimizado especificamente para o comportamento do armazenamento de objeto baseado em nuvem.
-- MAGIC
-- MAGIC O armazenamento de objetos é barato, durável, altamente disponível e efetivamente escalável e é construído para manipulação de metadados escaláveis. Um dos principais objetivos ao projetar o Delta Lake foi resolver o problema de retornar rapidamente consultas pontuais em alguns dos maiores e mais rápidos conjuntos de dados do mundo.
-- MAGIC
-- MAGIC O Delta Lake separa os custos de computação e armazenamento e fornece desempenho atimizado dos dados, independentemente da escala.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Delta Lake trás a garantia ACID para as transações de armazenamento de objetos
-- MAGIC - **A**tomicidade: todas as transações são bem sucedidas ou falham completamente.  
-- MAGIC - **C**onsistência: refere-se a como um determinado estado dos dados é observado por operações simultâneas. Obeserve que, dos componentes individuais da garantia ACID, este é o que mais varia entre os sistemas.
-- MAGIC - **I**solamento: refere-se a como as operações simultâneas entram em conflito umas com as outras. As gaantias de isolomento fornecidas pelo Dlta Lake diferem de outros sistemas.
-- MAGIC - **D**urabilidade: as alterações confirmadas são permanentes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Problemas resolvidos pelo ACID:
-- MAGIC 1. Difícil anexar dados
-- MAGIC 1. Modificação de dados existentes difícil
-- MAGIC 1. Jobs falhando no meio do caminho
-- MAGIC 1. Operações difíceis em Real Time
-- MAGIC 1. Caro manter versões de dados históricos
-- MAGIC
-- MAGIC Como esses problemas são resolvidos pelo ACID?
-- MAGIC
-- MAGIC - A consistência garantida para o estado no momento em que a anexação começa. 
-- MAGIC - Transação atômica e alta durabilidade.  
-- MAGIC - Os anexos não irão falhar devido a conflitos, mesmo ao gravar de várias fontes simultaneamente. 
-- MAGIC - Upserts permitem atualizações e exclusões com sintaxe simples como uma única transação atômica. 
-- MAGIC - As alterações não são confirmadas até que uma tarefa seja bem sucedida. 
-- MAGIC - Os Jobs irão falhar ou serão totalmente bem sucedidos.
-- MAGIC
-- MAGIC O Delta Lake permite o processamento atômico de transações em micro lote quase em tempo real por meio de uma integração estreita com o fluxo estruturado. Os logs de transação usados para garantir atomicidade, consistência e isolamento permitem consultas instantâneas, facilitando o time travel.
