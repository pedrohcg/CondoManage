import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 

spark = SparkSession.builder.appName("total-transacoes-data-tipo"). \
    config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4'). \
    getOrCreate()

load_dotenv()

access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# configura credenciais da aws
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key_id)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_access_key)

data = datetime.today().strftime('%Y-%m-%d')

# cria os dataframes
df_imoveis = spark.read.csv(f"s3a://data-lake-391218393304/raw/{data}/imoveis/", header=True)
df_transacoes = spark.read.csv(f"s3a://data-lake-391218393304/raw/{data}/transacoes/", header=True)

# faz join dos dataframes e soma as transações agrupando pela data da transação e pelo tipo do imóvel
df_total_transacoes_data_tipo = df_imoveis.join(df_transacoes, df_imoveis.imovel_id == df_transacoes.imovel_id). \
    select(df_imoveis.tipo, df_transacoes.data_transacao, df_transacoes.valor_transacao). \
    groupBy(df_imoveis.tipo, df_transacoes.data_transacao). \
    agg(F.sum(df_transacoes.valor_transacao).alias("total_transacoes_data_tipo"))

# verifica se o dataframe está vazio e escreve o resultado em disco no formato parquet
if df_total_transacoes_data_tipo.isEmpty() == False: 
    df_total_transacoes_data_tipo.coalesce(1).write.mode('overwrite').parquet(f"s3a://data-lake-391218393304/trusted/{data}/transacoes_data_tipo/")