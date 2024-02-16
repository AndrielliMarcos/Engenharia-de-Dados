# TRY, EXCEPT, FINALLY
try:
    8 + 's'
except TypeError:
    print('Operação não permitida')

##############################################################################
try:
    f = open('Manipulacao_de_arquivos/arquivos/testandoerros.txt', 'w')
    f.write('Gravando no arquivo.')
except IOError:
    print('Erro: arquivo não encontrado ou não pode ser salvo.')
else:
    print('Conteúdo gravado com sucesso.')
    f.close()

##############################################################################
try:
    f = open('Manipulacao_de_arquivos/arquivos/testandoerros', 'r')
    f.write('Gravando no arquivo.')
except IOError:
    print('Erro: arquivo não encontrado ou não pode ser salvo.')
else:
    print('Conteúdo gravado com sucesso.')
    f.close()

##############################################################################
try:
    f = open('Manipulacao_de_arquivos/arquivos/testandoerros.txt', 'w')
    f.write('Gravando no arquivo.')
except IOError:
    print('Erro: arquivo não encontrado ou não pode ser salvo.')
else:
    print('Conteúdo gravado com sucesso.')
    f.close()
finally:
    print('Comandos no bloco finally são sempre executados!')

##############################################################################
def askint():
    while True:
        try:
            val = int(input("Digite um número: "))
        except:
            print("Você não digitou um número: ")
            continue
        else:
            print("Obrigado por digitar um número!")
            break
        finally:
            print("Fim da execução!")
        print(val)

askint()
