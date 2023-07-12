from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# Crie uma sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Lista dos nomes dos arquivos
arquivos = [
    "/usr/app/vendas1.csv",
    "/usr/app/vendas2.csv",
    "/usr/app/vendas3.csv",
    "/usr/app/vendas4.csv",
    "/usr/app/vendas5.csv"
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

# Leia o arquivo de taxas
df_taxas = spark.read.csv("/usr/app/taxas.csv", header=True, inferSchema=True)

# Cruze as taxas por país com as vendas
df_vendas_taxas = df_unido.join(df_taxas, on="pais", how="left")
df_vendas_taxas = df_vendas_taxas.withColumn("valor_venda", col("valor") * col("taxa"))

# Agrupe por CPF e some as vendas
df_soma_vendas = df_vendas_taxas.groupBy("cpf").agg(sum("valor_venda").alias("total_vendas"))

# Salve os dados em um arquivo CSV
df_soma_vendas.coalesce(1).write.mode("overwrite").csv("/usr/app/output/soma_vendas", header=True)

# Exiba o DataFrame com a soma das vendas
df_soma_vendas.show()


# Conte o número de linhas repetidas por CPF
df_contagem_repeticoes = df_unido.groupBy("cpf").agg(count("*").alias("num_repeticoes"))

# Salve os dados em um arquivo CSV
df_contagem_repeticoes.coalesce(1).write.mode("overwrite").csv("/usr/app/output/contagem_repeticoes", header=True)

# Exiba o DataFrame com a contagem de repetições por CPF
df_contagem_repeticoes.show()
