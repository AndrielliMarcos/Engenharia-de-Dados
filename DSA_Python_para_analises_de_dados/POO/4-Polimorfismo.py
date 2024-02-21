# Superclasse
class Veiculo():
    def __init__(self, marca, modelo):
        # atributos do objeto dessa classe
        self.marca = marca
        self.modelo = modelo
    
    def acelerar(self):
        pass

    def frear(self):
        pass

# Subclasses
# O método construtor será utilizado somente na superclasse
class Carro(Veiculo):
    def acelerar(self):
        print('O carro está acelerando.')

    def frear(self):
        print('O carro está freando.')

class Moto(Veiculo):
    def acelerar(self):
        print('A moto está acelerando.')

    def frear(self):
        print('A moto está freando.')

class Aviao(Veiculo):
    def acelerar(self):
        print('O avião está acelerando.')

    def frear(self):
        print('O avião está freando.')

    def decolar(self):
        print('O avião está decolando.')

# Lista de objetos
lista_veiculos = [Carro("Porsche", "911 Turbo"), Moto("Honda", "Black"), Aviao("Boing", "757")]

# Percorrer a lista de veículos
for item in lista_veiculos:

    # método acelerar tem comportamento diferente dependendo do tipo de objeto
    item.acelerar()

    # chamar o método frear() para cada objeto
    item.frear()

    # O método decolar pe executado somente se o objeto dor do tipo Avião
    if isinstance(item, Aviao): # se o item for do tipo Avião
        item.decolar()
    
    print('---------------------------')
