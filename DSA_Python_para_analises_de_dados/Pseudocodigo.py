# Calcular a área de um paralelograma
    # Exiba "Bem vindo ao Calculador de área de paralelogramo"
    # Peça para o usuário inserir o comprimento da base
    # Armazene o comprimento da base em uma variável
    # Peça para o usuário inserir a altura
    # Armazene a altura em uma variável
    # Calcule a área do paralelogramo> base * altura
    # Armazene o resultado em uma variável
    # Exiba o resultado

# Calculadora simples
    # Frase de boas vindas ao sistema
    # Peça para o usuário digitar o primeiro valor
    # Armazene esse valor em uma variável
    # Peça para o usuário digitar o segundo valor
    # Armazene este valor em uma variável
    # Peça o usuário digitar qual a operação ele deseja fazer
        # soma +
        # subtração -
        # multiplicação *
        # divisão /
    # Armazene a operação em uma variável
    # Realize o calculo
    # Armazene o resultado em uma variável
    # Exiba o resultado

# Bubble Sorte: algoritmo de ordenação simples que funciona comparando cada elemento com o próximo,
# e trocando-se de lugar se eles estiverem em ordem incorreta. O algoritmo repete esse processo várias vezes, 
# até que todos os elementos estejam ordenados.
    # Verificar se valor 1 é maior que o valor 2
        # se sim: armazena valor 1 em uma variável auxiliar
        #         valor 1 passa a ser igual o valor 2
        #         valor 2 passa a ser igual a variável auxiliar
        # pula para o próximo numero
        # se não: pula para o próximo numero      

def sorted(iteravel):
    # Cria uma cópia do iterável para não modificar o original
    copia_iteravel = list(iteravel)
    
    # Loop para percorrer todos os elementos da lista
    for i in range(len(copia_iteravel)):
        # Loop para percorrer todos os elementos restantes
        for j in range(i + 1, len(copia_iteravel)):
            # Se o elemento atual for maior que o próximo, troca os elementos de posição
            if copia_iteravel[i] > copia_iteravel[j]:
                copia_iteravel[i], copia_iteravel[j] = copia_iteravel[j], copia_iteravel[i] # declaração multipla
    
    # Retorna a lista ordenada
    return copia_iteravel

# Lista de números
numeros = [5, 2, 7, 6, 3, 10, 9, 8, 1, 0, 4]

# Ordenar a lista de forma crescente usando a função sorted() personalizada
numeros_ordenados = sorted(numeros)

# Imprimir a lista ordenada
print(numeros_ordenados)

# declaração multipla
p1, p2, p3 = 'a', 'b', 'c'

print(p1)
print(p2)
print(p3)

# Em um dicionário colocamos pares de chave:valor. Com isso conseguimos amarrar um valor ao outro
estudante = {'Pedro':24, 'Ana':22, 'Jose':25}
print(estudante['Pedro'])