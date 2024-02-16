# Criando um dicionário
dict_guido = {'nome': 'Guido Van',
              'linguagem': 'Python',
              'similar': ['c', 'lisp'],
              'users': 100000}

# Percorrendo o dicionário
for k,v in dict_guido.items():
    print(k,v)

# Impotando o pacote JSON
import json

# Convertendo o dicionário para um objeto json
json.dumps(dict_guido)

# Criando um arquivo json
with open("Manipulacao_de_arquivos/arquivos/dados.json", "w") as arquivo:
    arquivo.write(json.dumps(dict_guido))

# Leitura de arquivos json
with open('Manipulacao_de_arquivos/arquivos/dados.json', 'r') as arquivo:
    texto = arquivo.read()
    dados = json.loads(texto)

# print(dados)
print(dados['nome'])