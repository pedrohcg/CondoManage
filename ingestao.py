import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Cria sessão do spark
spark = SparkSession.builder.appName("ingestao-dados"). \
    config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4'). \
    getOrCreate()

load_dotenv()

# variáveis de ambiente
database_ip = os.getenv("DATABASE_IP")
database_name = os.getenv("DATABASE_NAME")
port = os.getenv("PORT")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# configura credenciais da aws
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key_id)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_access_key)

data = datetime.today().strftime('%Y-%m-%d')

# cria os dataframes
df_condominios = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{database_ip}:{port}/{database_name}") \
    .option("dbtable", "condominios") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_moradores = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{database_ip}:{port}/{database_name}") \
    .option("dbtable", "moradores") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_transacoes = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{database_ip}:{port}/{database_name}") \
    .option("dbtable", "transacoes") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# imoveis vem do csv
df_imoveis = spark.read.csv("/opt/spark/data/imoveis.csv", header=True)

# verifica se os dataframes não são vazios e escreve os arquivos resultantes no data lake house
if df_condominios.isEmpty() == False: 
    df_condominios.coalesce(1).write.mode('overwrite').csv(f"s3a://data-lake-391218393304/raw/{data}/condominios/", header=True)

if df_moradores.isEmpty() == False:
    df_moradores.coalesce(1).write.mode('overwrite').csv(f"s3a://data-lake-391218393304/raw/{data}/moradores/", header=True)

if df_transacoes.isEmpty() == False:
    df_transacoes.coalesce(1).write.mode('overwrite').csv(f"s3a://data-lake-391218393304/raw/{data}/transacoes/", header=True)

if df_imoveis.isEmpty() == False:
    df_imoveis.coalesce(1).write.mode('overwrite').csv(f"s3a://data-lake-391218393304/raw/{data}/imoveis/", header=True)