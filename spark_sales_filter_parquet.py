from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# Crie uma sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Lista dos nomes dos arquivos
arquivos = [
    "/usr/app/vendas1.csv",
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

df_unido.write.parquet("/usr/app/exemplo.parquet")
