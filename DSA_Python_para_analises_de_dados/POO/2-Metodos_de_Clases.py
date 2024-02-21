# A classe criado anteriormente tem uma limitação: para todos os objetos criados na classe Livro
# o título e o isbn serão os mesmos, já que eles estão com valores fixos.
# Vamos colocar titulo e isbn como parâmetos, dessa forma, ao criar um novo livro
# será possível enviar o nome do titulo e isbn

# Criando a classe Livro com parâmetos no método construtor
class Livro():
    def __init__(self, titulo, isbn):
        self.titulo = titulo
        self.isbn = isbn
        print('Construtor chamado para criar um objeto desta classe.')
    
    def imprime(self, titulo, isbn):
        print(f'Este é livro {titulo} e seu ISBN é {isbn}')

# Criando o objeto Livro2 que é uma instância da classe livro
Livro2 = Livro("O Poder do Hábito", 77886611)
print(Livro2.titulo)
Livro2.imprime("O Poder do Hábito", 77886611)

# Criando a classe Algoritmo
class Algoritmo():
    def __init__(self, tipo_algoritmo):
        self.tipo = tipo_algoritmo
        print('Construtor chamado para criar um objeto desta classe.')

# Criando um objeto a partir da classe Algoritmo
algoritmo1 = Algoritmo('Deep Learning')

algoritmo2 = Algoritmo('Handom Forest')

# Chamando o atributo de cada objeto
print(algoritmo1.tipo)
print(algoritmo2.tipo)
print('***************************************************************************************')
# Criando a classe Circulo
class Circulo():

    # o valor de pi é constante, por isso não entra no construtor
    pi = 3.14

    # Quando um objeto desta classe fro criado, este método será executado e o valor default do raio será 5
    def __init__(self, raio = 5):
        self.raio = raio

    # Método que calcula a área
    def area(self):
        return (self.raio * self.raio) * Circulo.pi
    
    # Método para definir um novo raio
    def setRaio(self, novo_raio):
        self.raio = novo_raio

    # Método para obter o raio do círculo
    def getRaio(self):
        return self.raio
    
# Criando o objeto chamado circ, uma instância da classe Circulo()
circ = Circulo()

# Executando um método da classe Circulo()
# Não será passado nenhum parâmetro, então deve retornar o valor default
print(circ.getRaio())

# Criando outro objeto chamado circ1. Uma instância da classe Circulo()
# Agora será passado um parâmero, então deve retornar o valor passado
circ1 = Circulo(7)
print(circ1.getRaio())

# Imprimindo o raio
print(f'Área igual a: {circ.area()}')

# Gerando um novo valor para o raio
circ.setRaio(3)
print(f'Novo raio igual a: {circ.getRaio()}')
    
    