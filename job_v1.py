import sqlite3
import csv

# cria um banco de dados
conn = sqlite3.connect('producao.db')

# cria uma tabela para armazenar os dados de produção de alimentos
conn.execute('''CREATE TABLE IF NOT EXISTS producao (
             produto TEXT,
             quantidade INTEGER,
             preco_medio REAL,
             receita_total REAL
)''')

# grava os dados e fecha a conexão
conn.commit()
conn.close()
print('Banco criado')

# abre o arquivo csv com os dados de produção de alimentos
with open('producao_alimentos.csv', 'r') as file:

    # cria um leitor de csv para ler o arquivo
    reader = csv.reader(file)

    # pula a primeira linha, que contém o cabeçalho
    next(reader)

    # conecta ao banco de dados
    conn = sqlite3.connect('producao.db')

    # insere cada linha do arquivo na tabela do banco
    for row in reader:
        conn.execute('INSERT INTO producao (produto, quantidade, preco_medio, receita_total) VALUES (?, ?, ?, ?)', row)

    conn.commit()
    conn.close()

print ('Job concluído com sucesso!')        
