# Processamento de Dados de Voo com Apache Spark e Delta Tables

Este repositório contém os códigos e arquivos utilizados para realizar o processamento dos dados de Voo dos Estados Unidos em 2023. Os dados foram obtidos através do [Bureau of Transportation Statistic](https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr). As tecnologias utilizadas para realizar esse processamento são o **Apache Spark** e o formato de arquivos **Delta**.

A motivação para criação desse repositório é colocar em prática os conhecimentos adquiridos durante o Curso de [Apache Spark](https://theplumbers.com.br/apache-spark/) da Engenharia de Dados Academy. 

## Objetivo

O objetivo aqui é simular a criação de um Data Lakehouse com a arquitetura "Medalion", em que os dados são organizados em camadas Bronze, Silver e Gold. Durante esse processo foram utilizadas as tecnologias **Apache Spark** para processamento dos dados e arquivos no formato **Delta** para simular a construção de um Delta Lakehouse.


## Pré-Requisitos
O desenvolvimento desse projeto foi realizado com a utilização da linguagem **Python**, junto das bibliotecas ***pyspark*** e ***delta-spark***. 

O arquivo ***requirements.txt*** possui as bibliotecas utilizadas durante a construção desse projeto com o Spark. 

## Estrutura 
A estrutura desse projeto é simples e se baseia principalmente em duas pastas, ```docs``` e ```src```. 

Na pasta ````docs````, temos:

- **landing_zone**: 
    - **airport**: Essa pasta contém o arquivo com informações auxiliares sobre aeroportos. 
    - **planes**:  Essa pasta contém o arquivo com informações auxiliares sobre aeronaves. 
    - **flights**: Essa pasta contém os arquivos brutos dos dados de voo. As informações contidas nele são data do voo, origem, destino, tempo de atraso, motivo do cancelamento, horas de voo, etc. 

- **bronze_layer**: Aqui, ficam armazenadas as tabelas Delta geradas a partir dos dados brutos. As tabelas Delta, na camada Bronze, replicam a estrutura dos arquivos brutos, mas com a vantagem de serem armazenadas em um formato otimizado para consulta e análise.

- **silver_layer**: A pasta referente a camada silver armazena os arquivos Delta que tiveram um tratamento e limpeza nos dados. As transformações aplicadas foram:
    - Substituição de valores nulos por 0 em colunas numéricas.
    - Criação da coluna *"TOTAL_MINUTES_DELAY"* contendo a soma das colunas de atraso. 
    - Criação da coluna *"DATE_FLIGHT"* contendo a informação da data do voo no formato "YYYY-MM-DD".
    - Formatação das colunas de hora para um formato padrão "HH:mm".

- **gold_layer**: Por fim, temos a pasta com os arquivos Delta da camada Gold. Nessa camada, os dados estão agregados a fim de gerar insights consumiveis por áreas de negócio. As agregações feitas são:
    - Cálculo de métricas como: número total de voos, número de voos atrasados, tempo médio de atraso, distância total percorrida, tempo médio de voo, etc.
    - Agrupamento dos dados por data do voo, origem, destino e tipo de cancelamento.


Já na pasta ```src``` temos:

- **flights_bronze_layer.py**: Código PySpark para processamento da camada Bronze.

- **flights_silver_layer.py**: Código PySpark para processamento e transformação dos dados na camada Silver. Nesse script, estão a criação das colunas *"TOTAL_MINUTES_DELAY"* e *"DATE_FLIGHT"*.

- **flights_gold_layer.py**: Script PySpark com a leitura dos dados da camada silver e processamento na camada Gold. Aqui, contém a criação de um DataFrame agregado com as métricas relacionadas aos Voos e sumarizadas por Data, Origem, Destino e Tipo de Cancelamento.

Por fim, os dados entregues na camada ***Gold***, tem a seguinte estrutura de colunas com suas respectivas descrições:

| **Coluna**               | **Descrição**                                                                |
|--------------------------|------------------------------------------------------------------------------|
| date_flight              | Data em que o Voo aconteceu                                                  |
| origin                   | Código do aeroporto de origem do voo                                         |
| origin_airport           | Nome do aeroporto de origem do voo                                           |
| origin_city              | Nome da cidade em que o aeroporto de origem do voo esta localizada           |
| dest                     | Código do aeroporto de destino do voo                                        |
| dest_airport             | Nome do aeroporto de destino do voo                                          |
| dest_city                | Nome da cidade em que o aeroporto de destino do voo esta localizada          |
| cancellation_type        | Motivo do cancelamento do voo. Nulo quando não houve cancelamento            |
| total_flights            | Total de voos                                                                |
| total_flights_delayed    | Total de voos com atraso                                                     |
| sum_carrier_delay        | Total de minutos atrasados devido a operadora                                |
| mean_carrier_delay       | Média de minutos atrasados devido a operadora por voo                        |
| sum_weather_delay        | Total de minutos atrasados devido as condições climáticas                    |
| mean_weather_delay       | Média de minutos atrasados devido as condições climáticas por voo            |
| sum_nas_delay            | Total de minutos atrasados devido ao "National Air System"                   |
| mean_nas_delay           | Média de minutos atrasados devido ao "National Air System" por   voo         |
| sum_security_delay       | Total de minutos atrasados devido a questões de segurança                    |
| mean_security_delay      | Média de minutos atrasados devido a questões de segurança por voo            |
| sum_late_aircraft_delay  | Total de minutos atrasados devido ao pouso da aeronave utilizada             |
| mean_late_aircraft_delay | Média de minutos atrasados  devido   ao pouso da aeronave utilizada por voo  |
| sum_total_minutes_delay  | Total de minutos atrasados                                                   |
| mean_total_minutes_delay | Média de minutos atrasados por voo                                           |
| sum_distance             | Total da distancia percorrida                                                |
| mean_distance            | Média da distancia percorrida por voo                                        |
| sum_air_time             | Tempo total de voo                                                           |
| mean_air_time            | Média do tempo total de voo                                                  |
| sum_taxi_in              | Total de minutos gastos durante o "Taxi In"                                  |
| mean_taxi_in             | Média de minutos gastos durante o "Taxi In" por voo                          |
| sum_taxi_out             | Total de minutos gastos durante o "Taxi Out"                                 |
| mean_taxi_out            | Média de minutos gastos durante o "Taxi Out" por voo                         |



Após a execução dos scripts em PySpark e a criação dos arquivos Delta, os dados estariam prontos para serem consumidos por um Modern Data Warehouse, ou por uma ferramenta de BI, por exemplo. *No entanto, **nesse projeto**, o foco não é realizar esse tipo de disponibilização, mas sim focar na utilização do Spark para o processamento dos dados.*

# Conclusões

O desenvolvimento deste projeto de processamento de dados de voo com Apache Spark e Delta Tables foi um exercício importante, que permitiu a aplicação prática de conceitos relacionados ao Spark. As tecnologias utilizadas demonstraram sua efetividade no processamento e armazenamento de grandes volumes de dados de forma extremamente eficiente para o caso.

Através da execução das etapas de processamento das camadas Bronze, Silver e Gold, foi possível transformar os dados brutos em informações que estariam prontas para serem utilizadas. As métricas calculadas na camada Gold fornecem insights sobre o desempenho dos voos, tempos e motivos de atrasos, distâncias percorridas e tempo de voo.
