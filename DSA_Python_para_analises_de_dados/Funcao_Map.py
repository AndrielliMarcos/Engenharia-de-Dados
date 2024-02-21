# A função Map() em Python é uma função que aplica uma determinada função a cada elemento
# de uma estrutura de dados iterável (como uma lista, tupla ou outro objeto iterável).

# Função para retornar um número ao quadrado
def potencia(x):
    return x ** 2

numeros = [1, 2, 3, 4, 5]

# Vamos aplicar a função potência a cada número da lista de números
numero_ao_quadrado = list(map(potencia, numeros))
#print(numero_ao_quadrado)

## CRIANDO 2 FUNÇÕES
# Recebe uma temperatura como parâmetro e retorna a temperatura em Fahrenheit
def fahrenheit(t):
    return ((float(9)/5)*t + 32)

# Recebe uma temperatura como parâmetro e retorna a temperatura em Fahrenheit
def celsius(t):
    return (float(5)/9) * (t-32)

# Criando uma lista de temperaturas
temperaturas = [0, 22.5, 40, 10]

# Aplicar a função fahrenheit() a cada elemento da lista de temperatura
# map(fahrenheit, temperaturas) # Em Python 3 a função map() retorna um interator, qque pode ser convertida para lista
#list(map(fahrenheit, temperaturas))

# usando o loop para imprimir a lista de temperaturas convertidas
for temp in map(fahrenheit, temperaturas):
    print(temp)

# Convertendo para Celsius
map(celsius, temperaturas)
print(list(map(celsius, temperaturas)))

# Usar a expressão lambda na lista de temperaturas para fazer a conversão
map(lambda x: (5.0/9) * (x-32), temperaturas)
print(list(map(lambda x: (5.0/9) * (x-32), temperaturas)))

# Somando os elementos de 2 listas
a = [1,2,3,4,5]
b = [6,7,8,9,10]

print(list(map(lambda x,y: x+y, a, b)))