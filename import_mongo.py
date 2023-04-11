import pymongo                           # pip install pymongo
import pyarrow.parquet as pq             # pip install pyarrow
import pandas as pd


# Carregar o arquivo Parquet para um dataframe do pandas
table = pq.read_table('data.parquet')
df = table.to_pandas()

# Conectar ao servidor MongoDB local
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Criar uma base de dados chamada "db_bigdata"
my_db = client["db_bigdata"]

# Criar uma coleção chamada "inflation"
my_col = my_db["inflation"]


# Converter o dataframe numa lista de dicionários
documents = df.to_dict(orient='records')

# Inserir os documentos na coleção
my_col.insert_many(documents)
