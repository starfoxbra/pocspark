#!/bin/bash

start_time=$(date +%s)  # Obtém o tempo de início em segundos

# Remover o diretório "output" (se existir)
sudo rm -rf output

# Criar o diretório "output"
mkdir output

#elasticsearch
#sudo docker run --rm --name aoki_elastic -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" --network aoki_net docker.elastic.co/elasticsearch/elasticsearch:7.7.0

#docker system prune -a --volumes
#docker builder prune --all

# Construir a imagem Docker com a tag "aoki41"
sudo docker build -t aoki45 .

# Executar o contêiner Docker, mapeando o volume "spark_out" e a porta 8080
#sudo docker run  -v spark_out:/usr/app -p 8080:4040 aoki41
sudo docker run  --rm -v spark_out3:/usr/app -p 8080:4040 --network aoki_net aoki45
# Calcular o tempo total de execução
end_time=$(date +%s) 
execution_time=$((end_time - start_time))  # Calcula a diferença de tempo em segundos

# Imprimir o tempo total de execução
echo "Tempo total de execução: ${execution_time} segundos"
