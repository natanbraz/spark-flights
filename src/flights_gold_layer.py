# import libraries
from pyspark.sql import SparkSession
from delta import *
import pyspark.sql.functions as F

def main():

    # Inicia a sessão Spark com a configuração dos arquivos Delta
    builder = SparkSession.builder.appName("app") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Le o arquivo da camada silver e também arquivos auxiliares (plane-data e airports) que serão utilizados para construir a camada gold
    df = spark.read.format('delta').load('docs/silver_layer')
    planes = spark.read.option('header', 'true').csv('docs/landing_zone/flights/planes/plane-data.csv')
    airports = spark.read.option('header', 'true').csv('docs/landing_zone/flights/airports/airports.csv')

    # Realizando o join entre os datasets para construir uma tabela gold
    df_flight_planes = df.join(planes,
                            how='left',
                            on=df.TAIL_NUM == planes.tailnum,
                            )

    # Criando um alias para realizar o join entre os códigos de ORIGIN e os códigos dos aeroportos correspondentes 
    # e atribuindo o prefixo "origin_" para as colunas do novo DataFrame.
    origin_airports = airports.alias('origin_airports')
    origin_airports = origin_airports.select([F.col(c).alias('origin_'+c) for c in origin_airports.columns])

    df_flight_planes_airports = df_flight_planes.join(origin_airports,
                            how='left',
                            on=df.ORIGIN == origin_airports.origin_iata,
                            )

    # Criando um alias para realizar o join entre os códigos de DESTINATION e os códigos dos aeroportos correspondentes 
    # e atribuindo o prefixo "dest_" para as colunas do novo DataFrame.
    dest_airports = airports.alias('dest_airports')
    dest_airports = dest_airports.select([F.col(c).alias('dest_'+c) for c in dest_airports.columns])

    df_flight_planes_airports = df_flight_planes_airports.join(dest_airports,
                            how='left',
                            on=df.DEST == dest_airports.dest_iata,
                            )

    # Renomeando as colunas para que fiquem em lowercase
    df_gold = df_flight_planes_airports.select([F.col(c).alias(c.lower()) for c in df_flight_planes_airports.columns])

    # Criando a coluna cancellation_type com regra de negócio sobre os códigos de cancelamento
    df_gold = df_gold.withColumn('cancellation_type',
        F.when(F.col('cancellation_code')=='A', 'carrier')
        .when(F.col('cancellation_code')=='B', 'weather')
        .when(F.col('cancellation_code')=='C', 'national air system')
        .when(F.col('cancellation_code')=='D', 'security')
        .otherwise(None)
    )

    # Aplicando transformações para a criação do DataFrame Gold com a agregação de Voos por dia e Origin/Destino 
    # e com métricas básicas de média e soma para os voos.
    df_gold = df_gold.groupBy(
        'date_flight',
        'origin',
        'origin_airport',
        'origin_city',
        'dest',
        'dest_airport',
        'dest_city',
        'cancellation_type', 
    ).agg(
        F.count('*').alias('total_flights'),
        F.sum(F.when(F.col('total_minutes_delay') > 0, 1).otherwise(0)).alias('total_flights_delayed'),
        F.sum('carrier_delay').alias('sum_carrier_delay'),
        F.mean('carrier_delay').alias('mean_carrier_delay'),
        F.sum('weather_delay').alias('sum_weather_delay'),
        F.mean('weather_delay').alias('mean_weather_delay'),
        F.sum('nas_delay').alias('sum_nas_delay'),
        F.mean('nas_delay').alias('mean_nas_delay'),
        F.sum('security_delay').alias('sum_security_delay'),
        F.mean('security_delay').alias('mean_security_delay'),
        F.sum('late_aircraft_delay').alias('sum_late_aircraft_delay'),
        F.mean('late_aircraft_delay').alias('mean_late_aircraft_delay'),
        F.sum('total_minutes_delay').alias('sum_total_minutes_delay'),
        F.mean('total_minutes_delay').alias('mean_total_minutes_delay'),
        F.sum('distance').alias('sum_distance'),
        F.mean('distance').alias('mean_distance'),
        F.sum('air_time').alias('sum_air_time'),
        F.mean('air_time').alias('mean_air_time'),
        F.sum('taxi_in').alias('sum_taxi_in'),
        F.mean('taxi_in').alias('mean_taxi_in'),
        F.sum('taxi_out').alias('sum_taxi_out'),
        F.mean('taxi_out').alias('mean_taxi_out')
    )

    write_delta_mode = 'overwrite'

    df_gold.write.option("overwriteSchema", "true").mode(write_delta_mode).format('delta').save('docs/gold_layer')

if __name__=='__main__':
    main()
    