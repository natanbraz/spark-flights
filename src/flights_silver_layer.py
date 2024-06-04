from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType
from delta import *

def main ():

        
    builder = SparkSession.builder.appName("flights_silver_layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Local da tabela delta Bronze 
    flights_bronze = 'docs/bronze_layer/flights'

    # Lendo a tabela Delta e armazenando no DataFrame.
    df = spark.read.format('delta').load(flights_bronze)

    # Transformações no dataset da camada bronze

    # Criação um novo Dataframe com algumas transformações:
    # Substituindo para o valor 0 as colunas que estão com registros com valores nulos
    df_transformed = df.fillna(value=0,
                            subset=['ARR_DELAY', 'DEP_DELAY', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY'])


    # Criação de um campo de data com os valores de Year, Month e DayofMonth concatenados;
    # Formatação dos campos que indicam horários em que os eventos aconteceram
    df_transformed = df_transformed.select(
        'DAY_OF_WEEK',
        'OP_CARRIER_FL_NUM', 
        'TAIL_NUM',
        'OP_UNIQUE_CARRIER',
        'ACTUAL_ELAPSED_TIME',
        'CRS_ELAPSED_TIME',
        'AIR_TIME',
        'ORIGIN',
        'DEST',
        'ARR_DELAY',
        'DEP_DELAY',
        'DISTANCE',
        'TAXI_IN',
        'TAXI_OUT',
        'CANCELLED',
        'CANCELLATION_CODE',
        'DIVERTED',
        'CARRIER_DELAY',
        'WEATHER_DELAY',
        'NAS_DELAY',
        'SECURITY_DELAY',
        'LATE_AIRCRAFT_DELAY',
        (F.col('NAS_DELAY') + F.col('CARRIER_DELAY') + F.col('WEATHER_DELAY') + F.col('SECURITY_DELAY') + F.col('LATE_AIRCRAFT_DELAY')).alias('TOTAL_MINUTES_DELAY'),
        F.concat_ws('-', 'YEAR', 'MONTH', 'DAY_OF_MONTH').cast(DateType()).alias('DATE_FLIGHT'),
        F.concat_ws(':', F.substring(F.lpad('DEP_TIME', 4, '0'), 1, 2), F.substring(F.lpad('DEP_TIME', 4, '0'), 3, 4)).alias('DEP_TIME'),
        F.concat_ws(':', F.substring(F.lpad('CRS_DEP_TIME', 4, '0'), 1, 2), F.substring(F.lpad('CRS_DEP_TIME', 4, '0'), 3, 4)).alias('CRS_DEP_TIME'),
        F.concat_ws(':', F.substring(F.lpad('ARR_TIME', 4, '0'), 1, 2), F.substring(F.lpad('ARR_TIME', 4, '0'), 3, 4)).alias('ARR_TIME'),
        F.concat_ws(':', F.substring(F.lpad('CRS_ARR_TIME', 4, '0'), 1, 2), F.substring(F.lpad('CRS_ARR_TIME', 4, '0'), 3, 4)).alias('CRS_ARR_TIME')
        )

    write_delta_mode = 'overwrite'

    df_transformed.write.mode(write_delta_mode).format('delta').save('docs/silver_layer')

if __name__ == '__main__':
    main()
    