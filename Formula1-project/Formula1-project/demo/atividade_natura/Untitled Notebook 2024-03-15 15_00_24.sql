-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS natura


-- COMMAND ----------

-- Criação da tabela livros
CREATE TABLE natura.livros (
    id_livro INT,
    titulo VARCHAR(255),
    autor VARCHAR(255),
    ano_publicacao INT
);

-- Criação da tabela exemplares
CREATE TABLE natura.exemplares (
    id_exemplar INT,
    id_livro INT,
    disponivel BOOLEAN
    
);

-- Criação da tabela emprestimos
CREATE TABLE natura.emprestimos (
    id_emprestimo INT,
    id_exemplar INT,
    data_emprestimo DATE,
    data_devolucao DATE    
);


-- COMMAND ----------

-- Inserção de dados na tabela livros
INSERT INTO natura.livros (id_livro, titulo, autor, ano_publicacao)
VALUES 
    (1, 'Dom Casmurro', 'Machado de Assis', 1899),
    (2, 'Harry Potter e a Pedra Filosofal', 'J.K. Rowling', 1997),
    (3, '1984', 'George Orwell', 1949),
    (4, 'Memórias Póstumas de Brás Cubas', 'Machado de Assis', 1881),
    (5, 'Quincas Borba', 'Machado de Assis', 1891),
    (6, 'Vidas Secas', 'Graciliano Ramos', 1938),
    (7, 'São Bernardo', 'Graciliano Ramos', 1934);;

-- Inserção de dados na tabela exemplares
INSERT INTO natura.exemplares (id_exemplar, id_livro, disponivel)
VALUES
    (1, 1, FALSE),
    (2, 1, FALSE),
    (3, 2, TRUE),
    (4, 2, FALSE),
    (5, 2, FALSE),
    (6, 3, TRUE),
    (7, 4, TRUE),
    (8, 5, TRUE),
    (9, 5, FALSE);

-- Inserção de dados na tabela emprestimos
INSERT INTO natura.emprestimos (id_emprestimo, id_exemplar, data_emprestimo, data_devolucao)
VALUES
    (1, 1, '2024-03-10', NULL),
    (2, 2, '2024-03-09', NULL),
    (3, 3, '2024-03-08', '2024-03-14'),
    (4, 9, '2024-03-05', NULL),
    (5, 5, '2024-03-05', NULL),
    (6, 6, '2024-03-08', '2024-03-14');

-- COMMAND ----------

USE natura;

-- COMMAND ----------

SELECT * FROM livros;

-- COMMAND ----------

SELECT * FROM exemplares;

-- COMMAND ----------

SELECT * FROM emprestimos;

-- COMMAND ----------

-- DBTITLE 1,Listar os títulos dos livros que estão atualmente emprestados, juntamente com os nomes dos autores.
SELECT l.titulo, l.autor 
FROM exemplares e
JOIN livros l ON e.id_livro = l.id_livro
JOIN emprestimos em ON e.id_exemplar = em.id_exemplar
WHERE e.disponivel = FALSE AND em.data_devolucao IS NULL;


-- COMMAND ----------

-- DBTITLE 1,Encontrar o autor com o maior número de livros diferentes emprestados.
SELECT l.autor, COUNT(DISTINCT(titulo)) AS total_livros_emprestados
FROM exemplares e
JOIN livros l ON e.id_livro = l.id_livro
JOIN emprestimos em ON e.id_exemplar = em.id_exemplar
GROUP BY l.autor
ORDER BY total_livros_emprestados DESC
LIMIT 1;

-- COMMAND ----------

SELECT l.titulo, l.autor, em.data_emprestimo
FROM exemplares e
JOIN livros l ON e.id_livro = l.id_livro
JOIN emprestimos em ON e.id_exemplar = em.id_exemplar
WHERE em.data_emprestimo IS NULL;

-- COMMAND ----------

-- DBTITLE 1,Calcular a média de dias que os livros ficam emprestados, considerando apenas os livros que foram devolvidos.
SELECT l.titulo, l.autor, em.data_emprestimo, em.data_devolucao, DATEDIFF(em.data_devolucao, em.data_emprestimo) AS total_dias_emprestados
FROM exemplares e
JOIN livros l ON e.id_livro = l.id_livro
JOIN emprestimos em ON e.id_exemplar = em.id_exemplar
WHERE em.data_devolucao IS NOT NULL;


-- COMMAND ----------

SELECT AVG(DATEDIFF(em.data_devolucao, em.data_emprestimo)) AS media_dias_emprestados
FROM exemplares e
JOIN livros l ON e.id_livro = l.id_livro
JOIN emprestimos em ON e.id_exemplar = em.id_exemplar
WHERE em.data_devolucao IS NOT NULL;

-- COMMAND ----------

-- Script para criar a tabela pedidos
CREATE TABLE natura.pedidos (
    id_pedido INT,
    produto VARCHAR(255),
    quantidade INT,
    valor_unitario DECIMAL(10, 2)
);

-- COMMAND ----------

-- Script para inserir dados fictícios na tabela pedidos
INSERT INTO natura.pedidos (id_pedido, produto, quantidade, valor_unitario) VALUES
    (1, 'Camiseta', 5, 25.99),
    (2, 'Calça Jeans', 3, 59.99),
    (3, 'Tênis', 2, 89.99),
    (4, 'Boné', 10, 15.99),
    (5, 'Meias', 6, 9.99),
    (6, 'Camiseta', 12, 7.99),
    (7, 'Camiseta', 10, 12.99),
    (8, 'Boné', 3, 15.50),
    (9, 'Boné', 7, 13.39),
    (10, 'Boné', 17, 7.54);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("SELECT * FROM natura.pedidos")
-- MAGIC         

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC df = df.withColumn('valor_total', col('quantidade') * col('valor_unitario'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df.filter("produto == 'Camiseta' and quantidade > 10"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1 = df.filter((df.produto == 'Camiseta') & (df.quantidade > 10) )
-- MAGIC display(df1)
-- MAGIC # df.filter(df.produto == 'Camiseta').show()

-- COMMAND ----------

SELECT *, (quantidade * valor_unitario) AS valor_total
FROM pedidos

-- COMMAND ----------

SELECT produto, SUM(quantidade * valor_unitario) AS valor_total 
FROM pedidos
GROUP BY produto
HAVING valor_total > 100


-- COMMAND ----------

-- MAGIC %python
-- MAGIC pessoa = {
-- MAGIC     "nome": "João",
-- MAGIC     "idade": 30,
-- MAGIC     "cidade": "São Paulo",
-- MAGIC     "profissao": "Engenheiro"
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pessoa.keys()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pessoa.values()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if 'estado' in pessoa:
-- MAGIC     print('TRUE')
-- MAGIC else:
-- MAGIC     print('FALSE')