# As especificações gerais, que todos os objetos terão, estarão na super-classe
# O que for específico de cada objeto, entrará na sub-classe

# Criando a classe Animal => Super-classe
class Animal():

    # Método construtor
    def __init__(self):
        print('Animal criado.')

    def imprimir(self):
        print('Este é um animal.')

    def comer(self): 
        print('Hora de comer.')
    
    def emitir_som(self):
        pass
        # este método não defini nada. Mas já ficará criado para caso seja definido algo no futuro
        # é como se estivéssemos criando uma lista vazia

# Criando a classe Cachorro => Sub-classe
# class classe_filha(classe_pai)
class Cachorro(Animal):
    def __init__(self):
        # execute o construtor da super-classe
        Animal.__init__(self) 
        print('Objeto cachorro criado!')
    
    def emitir_som(self):
        print('Au Au!')
        # este método será sobrescrito, então valerá o retorno que estiver na classe filha

# Criando a classe Gato => Sub-classe
class Gato(Animal):
    def __init__(self):
        Animal.__init__(self)
        print('Objeto gato criado.')

    def emitir_som(self):
        print('Miau!')

# Criando um objeto do tipo Cachorro
rex = Cachorro()
rex.emitir_som()
rex.imprimir()
rex.comer()

# Criando um objeto do tipo Gato
zeze = Gato()
zeze.emitir_som()
zeze.comer()
