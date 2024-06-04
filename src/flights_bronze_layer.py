# importação das bibliotecas
from pyspark.sql import SparkSession
from delta import *

def main():
        
    # Configuração da aplicação Spark para lidar com as tabelas delta.
    builder = SparkSession.builder.appName("flights_bronze_layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Inicialização da sessão do Spark
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Especificação do caminho que contém os arquivos brutos de voos.
    path = 'docs/landing_zone/flights/flights/*.csv'

    df = spark.read.format('csv').option('inferSchema', 'true').option('header', 'true').csv(path)

    # Declaração da forma de escrita da tabela delta como overwrite.
    write_delta_mode = 'overwrite'

    # Escrita da tabela delta na camada bronze.
    df.write.mode(write_delta_mode).format('delta').save('docs/bronze_layer/flights/')

if __name__ == '__main__':
    main()
