# Ler o arquivo
f = open("Manipulacao_de_arquivos/arquivos/salarios.csv", "r")

# Chamar o método read e gravar em uma variável
data = f.read()

# Dividir o texto sempre que encontra um enter
rows = data.split('\n')
# print(rows)

# DIVIDIR UM ARQUIVO EM COLUNAS
f = open("Manipulacao_de_arquivos/arquivos/salarios.csv", "r")
data = f.read()
rows = data.split('\n')

# cria lista vazia
full_data = []

for row in rows:
    split_row = row.split(",") # dividi o texto por ,
    full_data.append(split_row)
    first_row = full_data[0]
# print(full_data)

# Contar quantas linhas tem o arquivo
count = 0
for row in full_data:
    count += 1 # Equivalente a: count = count + 1
print(count)

# Contar quantas colunas tem o arquivo
count_column = 0
for column in first_row:
    count_column += 1
print(count_column)
