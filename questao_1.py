# Import Bibliotecas

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import datetime 
from pyspark.sql.types import * 
from pyspark.sql.functions import * 


# Criando um Sessão no Spark

spark = SparkSession \
    .builder \
    .appName("Desafio STITDATA QUESTÃO 1") \
    .getOrCreate()


# Criando um variavel que recebe o path recerente ao arquivo que será carregado no datafrane

path_usados = './chatbot_usados.csv'


# Carregando arquivo CSV em um dataframe

dfChatUsados = spark.read.option("header", True)\
                        .option("inferSchema", True)\
                        .option("delimiter", "|")\
                        .csv(path_usados)


# Imprimindo o schema do dataframe

dfChatUsados.printSchema()


# Visualizando os dados no Dataframe

dfChatUsados.show()

# Criando um novo dataframe que itá receber a quantidade de clientes que executaram os BOt entre 1000 a 2000

dfChatUsadosTotal = dfChatUsados.filter(col('chatbot_id').between(1000, 2000)).count()

# Imprimindo o Resultado

print("A quantidade total de cliente que executaram bot's entre 1000 e 2000 é: {}".format(dfChatUsadosTotal))