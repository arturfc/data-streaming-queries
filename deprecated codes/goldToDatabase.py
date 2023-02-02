'''
    read gold parquet file
    save on database
    obs:
        on database, create views to answer business requirements
'''
#%%
from pyspark.sql import SparkSession
import psycopg2

#%%
#Connection details
PSQL_SERVERNAME = "127.0.0.1"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "test_database"
PSQL_USRRNAME = "postgres"
PSQL_PASSWORD = "123"

#jdbc:postgresql://127.0.0.1/test_database
URL = f"jdbc:postgresql://{PSQL_SERVERNAME}/{PSQL_DBNAME}"

#Table details
TABLE_MYTABLE = "data"
SCHEMA = "cliente1"

#%%
spark = (
    SparkSession.builder
    .appName("goldToDatabase")
    .getOrCreate()
)

df=(spark
    .read
    .format("parquet")
    .load("gold.parquet")
)
#%%
df_test = df.select(df.name)
#%%
df_test.write\
        .format("jdbc")\
        .option("url", URL)\
        .option("dbtable", "abc")\
        .option("driver", "org.postgresql.Driver") \
        .option("user", PSQL_USRRNAME)\
        .option("password", PSQL_PASSWORD)\
        .mode("append")\
        .save()
# %%
df_read = spark.read\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/test_database")\
        .option("dbtable", "abc")\
        .option("user", "postgres")\
        .option("password", "123")\
        .load()

