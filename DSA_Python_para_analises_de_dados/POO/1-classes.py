# Criando uma classe chamada Livro
class Livro():

    # método construtor
    # este método vai inicializar cada objeto criado a partir desta classe
    # (self) é uma referência a cada atributo da própria classe (e não de uma classe mãe, por exemplo)
    def __init__(self): 

        # atributos são propriedades
        self.titulo = "Sapiens - Uma breve história da humanidade."
        self.isbn = 9988888
        print("Construtor chamado para criar um objeto da classe.")

    # métodos são funções que executam ações nos objetos da classe
    def imprime(self):
        print(f"Foi criado o livro {self.titulo}, {self.isbn}")

# Criando uma instância da classe Livro
# Neste caso o método construtor será chamado
Livro1 = Livro()

# O objeto livro1 é do tipo Livro
print(type(Livro1))
# saída: <class '__main__.Livro'>

# Atributo do objeto Livro1
print(Livro1.titulo)

# Método do objeto Livro1
Livro1.imprime()