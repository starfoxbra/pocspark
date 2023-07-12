from faker import Faker
import random
import csv
import os

fake = Faker()

# Define o tamanho desejado em bytes (500 MB = 500,000,000 bytes)
tamanho_desejado = 2000000000

# Define o tamanho médio aproximado de cada registro em bytes
tamanho_registro = 200

# Calcula o número aproximado de registros necessários para atingir o tamanho desejado
num_registros = tamanho_desejado // tamanho_registro

# Cria uma lista de CPFs únicos
cpfs_unicos = [fake.unique.random_number(digits=11) for _ in range(num_registros)]

# Repete aleatoriamente alguns CPFs para garantir pelo menos 50 CPFs repetidos
cpfs_repetidos = random.choices(cpfs_unicos, k=50)

# Cria um arquivo CSV e escreve os registros gerados aleatoriamente
with open('vendas.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['nome', 'cpf', 'pais', 'data', 'bandeira', 'id_transacao', 'banco_pagamento', 'status_transacao', 'valor'])
    
    for _ in range(num_registros):
        nome = fake.name()
        
        # Seleciona aleatoriamente um CPF repetido ou um CPF único
        cpf = random.choice(cpfs_repetidos) if random.random() < 0.1 else random.choice(cpfs_unicos)
        
        pais = fake.country()
        data = fake.date_time_between(start_date='-5y', end_date='now')
        bandeira = random.choice(['mastercard', 'visa', 'elo'])
        id_transacao = fake.unique.random_number(digits=8)
        banco_pagamento = random.choice(['itau', 'bradesco', 'banco do brasil'])
        status_transacao = random.choice(['aprovada', 'negada', 'estornada'])
        valor = random.randint(100, 10000)
        
        writer.writerow([nome, cpf, pais, data, bandeira, id_transacao, banco_pagamento, status_transacao, valor])

# Verifica o tamanho do arquivo gerado
tamanho_arquivo = os.path.getsize('vendas.csv')
print(f"Tamanho do arquivo gerado: {tamanho_arquivo} bytes")
