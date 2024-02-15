# [expressão FOR item IN iterable IF condição == True]
# é um loop dentro de uma lista
# retorna uma expressão PARA cada item DENTRO de um iterable (coleção de números) SE a condição for verdadeira

# imprimi os números até 10
print( [x for x in range(10)] ) # retorne x PARA cada valor de x NA lista de elementos
print('---------------------------------------')

lista_mumeros = [x for x in range(10) if x < 5]
# retorne x PARA cada valor de x na lista de elemntos SE o valor de x for menos que 5
print(lista_mumeros)
print('---------------------------------------')

lista_frutas = ['banana', 'abacate', 'melancia', 'cereja', 'manga']
nova_lista = []
nova_lista2 = []

# loop tradicional para buscar as palavras com "m"
for x in lista_frutas:
    if "m" in x:
        nova_lista.append(x)
print(nova_lista)
print('---------------------------------------')

nova_lista2 = [x for x in lista_frutas if "m" in x]
print(nova_lista2)
print('---------------------------------------')

# Dict Comprehension

dict_alunos = { 'Bob': 68, 'Michel': 84, 'Zico': 57, 'Ana': 93}

#criar um novo dicionario imprimindo os pares de chave:valor
dict_alunos_status = {k:v for (k, v) in dict_alunos.items()}
# retorna chave:valor PARA cada chave e valor retornado dos items do dict_alunos
print(dict_alunos_status)
print('---------------------------------------')

# mostrar se o aluno foi aprovado ou reprovadi
dict_alunos_status2 = {k: ('Aprovado' if v > 70 else 'Reprovado') for (k, v) in dict_alunos.items()}
print(dict_alunos_status2)