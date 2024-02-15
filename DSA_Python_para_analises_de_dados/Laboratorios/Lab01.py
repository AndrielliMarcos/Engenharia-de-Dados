# Gerar uma lista de números entre 1 e 100 que sejam pares e divisíveis por 4
numeros_pares_divisiveis_por_4 = [num for num in range(1, 101) if num % 2 == 0 and num % 4 == 0]

# Imprimir a lista resultante
print(numeros_pares_divisiveis_por_4)

# OBS: para o código acima foi usado o LIST COMPREHENSION
# O LIST COMPREHENSION traz um código em uma única linha dentro de []


