-- valores de venda min, max, média, soma e total por produtos => GROUP BY
-- ordenar pela coluna Contagem => ORDER BY
SELECT Produto, 
	ROUND(MIN(Valor_Venda), 2)  AS Valor_Minimo, 
	ROUND(MAX(Valor_Venda), 2) AS Valor_Maximo, 
	ROUND(AVG(Valor_Venda), 2) AS Valor_Media, 
	ROUND(SUM(Valor_Venda), 2) AS Valor_Total, 
	COUNT(Valor_Venda) AS Contagem 
FROM TB_DSA_VENDAS
GROUP BY Produto
ORDER BY Contagem DESC;

-- retornar a tabela com o nome do produto, e não com o código como retornado na query acima => JOIN
-- retornar o ano que a venda foi realizada => JOIN
SELECT Nome_Produto, 
	   ROUND(MIN(Valor_Venda), 2)  AS Valor_Minimo, 
       ROUND(MAX(Valor_Venda), 2) AS Valor_Maximo, 
	   ROUND(AVG(Valor_Venda), 2) AS Valor_Media, 
	   ROUND(SUM(Valor_Venda), 2) AS Valor_Total, 
 	   COUNT(Valor_Venda) AS Contagem,
 	   Ano
FROM TB_DSA_VENDAS, TB_DSA_PRODUTOS, TB_DSA_PEDIDOS
WHERE TB_DSA_VENDAS.Produto = TB_DSA_PRODUTOS.ID_Produto 
  AND TB_DSA_VENDAS.Pedido =  TB_DSA_PEDIDOS.ID_Pedido 
GROUP BY Produto, Ano
ORDER BY Contagem DESC;
