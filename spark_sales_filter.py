from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# Crie uma sessão do Spark
spark = SparkSession.builder.getOrCreate()


# Lista dos nomes dos arquivos
arquivos = [
    "/usr/app/vendas0.csv",
    "/usr/app/vendas1.csv",
    "/usr/app/vendas2.csv",
    "/usr/app/vendas3.csv",
    "/usr/app/vendas4.csv",
    "/usr/app/vendas6.csv"
]

# DataFrame vazio para unir os resultados
df_unido = None

# Leia os arquivos e crie os DataFrames individualmente
for arquivo in arquivos:
    df_vendas = spark.read.csv(arquivo, header=True, inferSchema=True)
    if df_unido is None:
        df_unido = df_vendas
    else:
        df_unido = df_unido.union(df_vendas)

# Agrupe por CPF e some as vendas
df_soma_vendas = df_unido.groupBy("cpf").agg(sum(col("valor")).alias("total_vendas"))

# Salve os dados da soma de vendas em um arquivo CSV
df_soma_vendas.write.csv("/usr/app/soma_vendas.csv", header=True)

# Exibe o DataFrame com a soma das vendas
df_soma_vendas.show()

# Filtro para considerar apenas registros do Brasil
df_brasil = df_unido.filter(col("banco_pagamento") == "itau")

# Salve os dados do Brasil em um arquivo CSV
df_brasil.write.csv("/usr/app/vendas_itau.csv", header=True)

# Exibe o DataFrame com os registros do Brasil
df_brasil.show()
