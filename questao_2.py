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
    .appName("Desafio STITDATA QUESTÃO 2") \
    .getOrCreate()


# Criando as variaveis que recebem os path referentes aos arquivos que seram carregado no datafrane

path_cliente = './chatbot_users.csv'

path_custos = './chatbot_custos.csv'


# Carregando arquivo CSV em um dataframe dos clientes

dfCliente = spark.read.option("header", True)\
                        .option("inferSchema", True)\
                        .option("delimiter", "|")\
                        .csv(path_cliente)

# Imprimindo o schema do dataframe

dfCliente.printSchema()


# Visualizando os dados no Dataframe

dfCliente.show()


# Carregando arquivo CSV em um dataframe dos custos

dfCustos = spark.read.option("header", True)\
                        .option("inferSchema", True)\
                        .option("delimiter", "|")\
                        .csv(path_custos)\
                        .withColumn("cost", regexp_replace("cost", "\\,", ".").cast("Double").alias('cost'))

# Imprimindo o schema do dataframe

dfCustos.printSchema()


# Visualizando os dados no Dataframe

dfCustos.show()


# Criando o join entre os dataframes de cliente e custos


dfClienteCusto = dfCliente.join(dfCustos, on=['chatbot_id']).select("customer_id", dfCliente.chatbot_id, "chatbot_type", "cost")

# Visualizando o dataframe com o resultado do join

dfClienteCusto.show()

# Realizando a consulta referente aos clientes, os quais a média do custo é maior que R$ 500,00 e armazenado em uma variavel como uma linha.

dfMaiorConsumo = dfClienteCusto.select("customer_id","chatbot_type", "cost").filter(col('cost') >= 500).collect()

#Imprimindo o resultado

print(f'Cliente(s) com a media maior que R$: 500.00!\n')

print(f'Cliente: {dfMaiorConsumo[0][0]} - tipo do chatbot: {dfMaiorConsumo[0][1]} - Valor Total (R$): {dfMaiorConsumo[0][2]}')