# Função com 3 linhas de código
def potenciaTres(num):
    resultado = num ** 2
    return resultado

print(potenciaTres(5))
print('-----------------------------------------')

# Função com 2 linhas de código
def potenciaDuas(num):
    return num ** 2

print(potenciaDuas(5))
print('-----------------------------------------')

# Função com 1 linhas de código
def potenciaUma(num): return num ** 2

print(potenciaUma(5))
print('-----------------------------------------')

# Definindo uma expressão lambda (função anônima)
# a fubção lambda cria uma função em tempo de execução. Não é necessário dar nome à função
potencia = lambda num: num ** 2 # a função lambda vai receber um numero qualquer e retornar este numero elevado a 2
# para facilitar a didática atribuímos o resultado a uma variável. Mas isso não é necessário
print(potencia(5)) 
print('-----------------------------------------')

par = lambda x: x % 2 == 0
print(par(4))
print('-----------------------------------------')

print(par(3))
print('-----------------------------------------')

addNum = lambda x,y : x + y
print(addNum(2,3))

