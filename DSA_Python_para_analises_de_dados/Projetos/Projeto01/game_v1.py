## JOGO DA FORCA
# print('Bem-vindo(a) ao jogo da forca! \nAdivinhe a palavra abaixo:')
# print('- - - - - - ')
# palavra = 'banana'

# i = 6
# letra_errada = [] 
# while 6 >= i >= 1:
#     print(f'Chances restantes: {i}')       

#     print(f'Letras erradas: {letra_errada}')    

#     letra = input('Digite uma letra: ')
    
#     if letra in palavra:
#         print(f"A letra '{letra}' está na palavra!")
#     else:
#         letra_errada.append(letra)
#         print(f"A letra '{letra}' não está na palavra!")
#     i -= 1
#########################################################################################

# import
import random #será usado para escolher uma palavra de maneira aleatória
from os import system, name

# Função para limpar a tela a cada execução
def limpa_tela():

    # Windows
    if name == 'nt': #'nt' é o nome interno do Windows 
        _ = system('cls')
    
    # Mac ou Linux
    else:
        _ = system('clear')

# Função que gra o jogo
def game():

    limpa_tela()

    print("\nBem-vindo(a) ao jogo da forca!")
    print("Adivinhe a palavra abaixo: \n")

    # Lista de palavras para o jogo
    palavras = ['banana', 'abacate', 'uva', 'morango', 'laranja']

    # Escolhe randomicamente uma palavra dentro da lista de palavras
    palavra = random.choice(palavras)

    # List comprehension para retornar _ para cada letra da palavra escolhida
    letras_descobertas = ['_' for letra in palavra]

    # Número de chances 
    chances = 6

    # Lista para as letras erradas
    letras_erradas = []

    # Loop enquanto número de chances for maior do que zero
    while chances > 0:
        print(" ".join(letras_descobertas)) # o join junta o espaço com letras_descobertas
        print("\nChances restantes: ", chances)
        print("Letras Erradas: ", " ".join(letras_erradas))

        tentativa = input("\nDigite uma letra: ").lower()

        # Verificar se a letra digitada está na palavra
        if tentativa in palavra:
            index = 0
            for letra in palavra:
                if tentativa == letra:
                    letras_descobertas[index] = letra # 'troca' o _ pela letra para mostrar para o usuário
                index += 1    
        else:
            chances -= 1 # as chances são diminuídas somente quando o usuário erra a letra
            letras_erradas.append(tentativa)

        # Checar se ainda existe '_' dentro da lista letras_descobertas
        # Se não existir mais '_', significa que o usuário acertou a palavra
        if "_" not in letras_descobertas:
            print("\nVocê venceu! A palavra era: ", palavra)   
            break
    # Checar se ainda existe '_' dentro da lista letras_descobertas
    # Se existir '_', significa que o usuário errou a palavra
    if "_" in letras_descobertas:
        print("\nVocê perdeu! A palavra era: ", palavra)

# Bloco main que indica que esse é um programa Python
if __name__ == "__main__":
    game()
    print("\nParabéns! Continue estudando Python. :) \n")