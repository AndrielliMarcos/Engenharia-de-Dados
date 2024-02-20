## JOGO DA FORCA
print('Bem-vindo(a) ao jogo da forca! \nAdivinhe a palavra abaixo:')
print('- - - - - - ')
palavra = 'banana'

i = 6
letra_errada = [] 
while 6 >= i >= 1:
    print(f'Chances restantes: {i}')       

    print(f'Letras erradas: {letra_errada}')    

    letra = input('Digite uma letra: ')
    
    if letra in palavra:
        print(f"A letra '{letra}' está na palavra!")
    else:
        letra_errada.append(letra)
        print(f"A letra '{letra}' não está na palavra!")
    i -= 1
    
    