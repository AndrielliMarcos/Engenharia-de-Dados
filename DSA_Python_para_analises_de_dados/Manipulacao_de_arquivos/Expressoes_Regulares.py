# Expressões regulares são padrões usados para combinar ou encontrar ocorrências
# de sequências de caracteres em uma string.

# importar o pacote re
import re

texto = 'Meu e-mail é exemplo@gmail.com e você pode me contratar em outro_email@yahoo.com'

# Expressão regular para contar quantas vezes o caravter arroba aparece no texto
resultado = len(re.findall('@', texto))
print(f'O caractere @ apareceu {resultado} vezes no texto.')
print('---------------------------------------------')

# Expressão regular para extrair a palavra que aparece após a palavra "você" em um texto
resultado1 = re.findall(r'você (\w+)', texto)
print(f"A palavra após 'você' é: {resultado1[0]}")