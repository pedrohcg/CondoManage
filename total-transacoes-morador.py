import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 

spark = SparkSession.builder.appName("total-transacoes-morador"). \
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
df_moradores = spark.read.csv(f"s3a://data-lake-391218393304/raw/{data}/moradores/", header=True)
df_transacoes = spark.read.csv(f"s3a://data-lake-391218393304/raw/{data}/transacoes/", header=True)

# faz join dos dataframes e soma as transações agrupando pelo id do morador e nome
df_total_transacoes_morador = df_moradores.join(df_transacoes, df_moradores.morador_id == df_transacoes.morador_id). \
    select(df_moradores.morador_id, df_moradores.nome, df_transacoes.valor_transacao). \
    groupBy(df_moradores.morador_id, df_moradores.nome). \
    agg(F.sum(df_transacoes.valor_transacao).alias("total_transacoes_morador"))

# verifica se o dataframe está vazio e escreve o resultado em disco no formato parquet
if df_total_transacoes_morador.isEmpty() == False: 
    df_total_transacoes_morador.coalesce(1).write.mode('overwrite').parquet(f"s3a://data-lake-391218393304/trusted/{data}/transacoes_morador/")