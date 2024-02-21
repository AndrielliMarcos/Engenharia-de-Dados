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
    