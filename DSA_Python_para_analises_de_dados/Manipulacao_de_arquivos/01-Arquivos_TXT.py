# Definindo o caminho do arquivo
# caminho_arquivo = r'C:/Users/BlueShift/OneDrive/Documentos/Estudos_Eng_Dados/Engenharia-de-Dados/DSA_Python_para_analises_de_dados/Manipulacao_de_arquivos/arquivos/arquivo1.txt'

# # Abrindo o arquivo em modo de leitura
# with open(caminho_arquivo, 'r') as arquivo:
#     # Lendo o conteúdo do arquivo
#     conteudo = arquivo.read()

# # Imprimindo o conteúdo do arquivo
# print(conteudo)

# Abrindo o arquivo para leitura
arq1 = open("Manipulacao_de_arquivos/arquivos/arquivo1.txt", "r")
print(arq1.read())

# Contar o número de caracteres
print(arq1.tell())

# Retornar para o início do arquivo
print(arq1.seek(0,0))

# Lendo os primeiros 23 caracteres
print(arq1.read(23))

### O modo de leitura (read = r) abre o arquivo somente para leitura, não é possível alterar este arquivo aberto para leitura.

## GRAVANDO ARQUIVOS
# Abrindo arquivo para gravação
# Se o arquivo não existe, o programa irá vriar o arquivo. Se o arquivo existir, ele irá sobrescrever o arquivo.
arq2 = open("Manipulacao_de_arquivos/arquivos/arquivo2.txt", "w") # w = write

# Gravando algo no arquivo
arq2.write("Aprendendo a programar em Python.")

# Fechar o método write
arq2.close()

# Lendo o arquivo gravado
arq2 = open("Manipulacao_de_arquivos/arquivos/arquivo2.txt", "r")
print(arq2.read())

# Acrescentando conteúdo
arq2 = open("Manipulacao_de_arquivos/arquivos/arquivo2.txt", "a") # a = append
arq2.write(" E a metodologia de ensino da Data Science Academy facilita o aprendizado")

# Fechar o método write
arq2.close()

# Lendo o arquivo gravado
arq2 = open("Manipulacao_de_arquivos/arquivos/arquivo2.txt", "r")
print(arq2.read())