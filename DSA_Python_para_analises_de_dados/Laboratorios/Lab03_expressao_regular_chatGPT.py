musica = '''
Todos os dias quando acordo
Não tenho mais
O tempo que passou
Mas tenho muito tempo
Temos todo o tempo do mundo
Todos os dias
Antes de dormir
Lembro e esqueço
Como foi o dia
Sempre em frente
Não temos tempo a perder
Nosso suor sagrado
É bem mais belo
Que esse sangue amargo
E tão sério
E selvagem! Selvagem!
Selvagem!
Veja o sol
Dessa manhã tão cinza
A tempestade que chega
É da cor dos teus olhos
Castanhos
Então me abraça forte
E diz mais uma vez
Que já estamos
Distantes de tudo
Temos nosso próprio tempo
Temos nosso próprio tempo
Temos nosso próprio tempo
Não tenho medo do escuro
Mas deixe as luzes
Acesas agora
O que foi escondido
É o que se escondeu
E o que foi prometido
Ninguém prometeu
Nem foi tempo perdido
Somos tão jovens
Tão jovens! Tão jovens!
'''

import re

# 1 - Crie um REGEX para contar quantas vezes o caractere "a" aparece em todo o texto da música
# Expressão regular para corresponder à letra 'a'
expressao_regular = r'a'

# Encontrar todas as correspondências da expressão regular no texto
correspondencias = re.findall(expressao_regular, musica)

# Contar quantas vezes a letra 'a' aparece no texto
quantidade_de_a = len(correspondencias)

# Imprimir o resultado
print("A letra 'a' aparece", quantidade_de_a, "vezes no texto.")
print('------------------------------------------------------------------')

# 2 - Crie um REGEX para contar quantas vezes a palavra tempo aparece na música
# Expressão regular para corresponder à palavra 'tempo'
expressao_regular = r'\btempo\b'

# Encontrar todas as correspondências da expressão regular no texto
correspondencias = re.findall(expressao_regular, musica)

# Contar quantas vezes a palavra 'tempo' aparece no texto
quantidade_de_tempo = len(correspondencias)

# Imprimir o resultado
print("A palavra 'tempo' aparece", quantidade_de_tempo, "vezes no texto.")
print('------------------------------------------------------------------')

# 3 - Crie um REGEX para extrair as palavras seguidas por exclamação
# Expressão regular para corresponder a sequências de palavras seguidas por um ponto de exclamação
expressao_regular = r'\b\w+\b!'

# Encontrar todas as correspondências da expressão regular no texto
correspondencias = re.findall(expressao_regular, musica)

# Imprimir o resultado
print("Sequências seguidas por exclamação encontradas:")
for sequencia in correspondencias:
    print(sequencia)
print('------------------------------------------------------------------')

# 4 - Crie um REGEX que extrai qualquer palavra cujo antecessor seja a palavra "esse" e o sucessor seja a palavra "amargo"
# Expressão regular para corresponder a uma palavra precedida por "esse" e seguida por "amargo"
expressao_regular = r'\besse\s+(\w+)\s+amargo\b'

# Encontrar todas as correspondências da expressão regular no texto
correspondencias = re.findall(expressao_regular, musica)

# Imprimir o resultado
print("Palavras precedidas por 'esse' e seguidas por 'amargo' encontradas:")
for palavra in correspondencias:
    print(palavra)
print('------------------------------------------------------------------')

# 5 - Crie um REGEX que retorne as palavras com acento, mas somente os caracteres na palavra que são anteriores ao caracter com acento
# Expressão regular para encontrar palavras com acento e retornar somente os caracteres anteriores ao acento
expressao_regular = r'\b(\w+)(?=[áéíóúÁÉÍÓÚâêîôûÂÊÎÔÛãõÃÕçÇ])'

# Encontrar todas as correspondências da expressão regular no texto
correspondencias = re.findall(expressao_regular, musica)

# Imprimir o resultado
print("Palavras com acento e caracteres anteriores ao acento:")
for palavra in correspondencias:
    print(palavra)