# Regra de negócio: carregar somente registros com quantidade produzida superior a 10

import csv
import sqlite3

# Abre o arquivo CSV com os dados da produção de alimentos
with open('producao_alimentos.csv', 'r') as file:

    # leitura do arquivo csv
    reader = csv.reader(file)

    # pula a primeira linha, que contém o cabeçalho do arquivo
    next(reader)

    # conecta ao banco de dados
    conn = sqlite3.connect('producao.db')

    # deleta a tabela no banco, caso exista uma
    conn.execute('DROP TABLE IF EXISTS producao')
    # uma outra opção é trincar a tabela. Mas depensendo do tamanho da tabela, é melhor deletá-la
    
    # cria uma tabela para armazenar os dados de produção de alimentos
    conn.execute('''CREATE TABLE producao (
               produto TEXT,
               quantidade INTEGER,
               preco_medio REAL,
               receita_total REAL  
    )''')
    
    # insere cada linha do arquivo com quantidade maior que 10 na tabela do banco de dados
    for row in reader:
        # verifica se a coluna quantidade é maior que 10
        if int(row[1]) > 10:
            conn.execute('INSERT INTO producao (produto, quantidade, preco_medio, receita_total) VALUES (?, ?, ?, ?)', row)

    conn.commit()
    conn.close()        

    print('Job concluído com sucesso')