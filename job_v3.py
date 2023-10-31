# Regra de negócio: Remover o caractere "ponto" na última coluna do arquivo para evitar que o número perca os últimos dígitos

import csv
import sqlite3

# Função para remover o ponto por espaço vazio nos dados da última coluna
def remove_ponto(valor):
    return int(valor.replace('.', ''))

# Abre o arquivo CSV com os dados da produção de alimentos
with open('producao_alimentos.csv', 'r') as file:

    # cria um leitor de csv para ler o arquivo
    reader = csv.reader(file)

    # pula a linha do cabeçalho
    next(reader)

    # conecta ao banco de dados
    conn = sqlite3.connect('producao.db')

    # deleta a tabela, se existir
    conn.execute('DROP TABLE IF EXISTS producao')
   
    # cria uma nova tabela para armazenar os dados de produção de alimentos
    conn.execute('''CREATE TABLE producao (
               produto TEXT,
               quantidade INTEGER,
               preco_medio REAL,
               receita_total REAL  
    )''')
    
    # insere cada linha do arquivo com quantidade maior que 10 na tabela do banco de dados
    for row in reader:
        if int(row[1]) > 10:

            # remove o ponto do valor com quantidade maior que 10 na tabela
            row[3] = remove_ponto(row[3])

            # insere o registro no banco de dados
            conn.execute('INSERT INTO producao (produto, quantidade, preco_medio, receita_total) VALUES (?, ?, ?, ?)', row)

    conn.commit()
    conn.close()

    print('Job concluído com sucesso!')        
