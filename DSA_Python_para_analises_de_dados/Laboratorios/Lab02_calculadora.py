print('****************************** Calculadora em Python ******************************\n')

def titulo():
    print('Selecione o número da operação desejada:\n')
    print('1 - Soma \n2 - Subtração \n3 - Multiplicação \n4 - Divisão \n')
        
def soma(a, b):
        return a + b

def subtracao(a, b):
    return a - b

def multiplicacao(a, b):
    return a * b

def divisao(a, b):
    return a / b

titulo()

operacao = input('Digite sua opção (1/2/3/4): ')
while operacao:
    if (operacao != '1') and (operacao != '2') and (operacao != '3') and (operacao != '4'):
        print('Opção inválida! Verifique as opções disponíveis.\n')        
        titulo()
        operacao = input('Digite sua opção (1/2/3/4): ')
        
    else:
        break

num1 = int(input('Digite o primeiro número: '))
num2 = int(input('Digite o segundo número: '))   

if operacao == '1':
    resultado = soma(num1, num2)
    print(f'\n{num1} + {num2} = {resultado}')
elif operacao == '2':
    resultado = subtracao(num1, num2)
    print(f'\n{num1} - {num2} = {resultado}')
elif operacao == '3':
    resultado = multiplicacao(num1, num2)
    print(f'\n{num1} * {num2} = {resultado}')
elif operacao == '4':
    resultado = divisao(num1, num2)
    print(f'\n{num1} / {num2} = {resultado}')


  