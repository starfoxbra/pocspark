from pyspark.sql import SparkSession

# Crie uma sessão do Spark
spark = SparkSession.builder.getOrCreate()

# Configurações do Elasticsearch
es_write_conf = {
    "es.nodes": "aoki_elastic",
    "es.port": "9200",
    "es.resource": "aoki/_doc",  # Substitua "aoki" pelo nome do índice desejado
    "es.mapping.id": "id_transacao"  # Substitua "cpf" pelo nome do campo que você deseja usar como ID
}

# Lista com o nome do arquivo de entrada desejado
arquivos = ["/usr/app/vendas0.csv"]

# DataFrame vazio para unir os resultados
df_unido = None

# Leia o arquivo e crie o DataFrame
for arquivo in arquivos:
    df_vendas = spark.read.csv(arquivo, header=True, inferSchema=True)
    df_unido = df_vendas

# Escreva os dados no Elasticsearch usando a biblioteca elasticsearch
df_unido.write.format("org.elasticsearch.spark.sql") \
    .options(**es_write_conf) \
    .mode("append") \
    .save()

# Exibe o DataFrame unido
df_unido.show()
