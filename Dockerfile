# Specify a base image
FROM apache/spark-py


USER root

WORKDIR /usr/app
COPY elasticsearch-spark-30_2.12-7.14.1.jar /opt/spark/jars/
# Instala o pacote pandas

COPY vendas1000mb.csv /usr/app/vendas0.csv
COPY vendas500mb.csv /usr/app/vendas1.csv
COPY vendas.csv /usr/app/vendas2.csv
COPY vendas1000mb.csv /usr/app/vendas3.csv
COPY vendas500mb.csv /usr/app/vendas4.csv
COPY vendas1000mb.csv /usr/app/vendas5.csv
COPY vendas500mb.csv /usr/app/vendas6.csv





COPY spark_sales_filter.py /usr/app/spark_sales_filter.py
COPY spark_sales_filter_parquet.py /usr/app/spark_sales_filter_parquet.py

# Execute o código Python e salve o arquivo de saída no diretório /output
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--deploy-mode", "client", "/usr/app/spark_sales_filter.py"]
