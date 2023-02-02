'''
    read silver parquet file
    do tranformation acording to business rules defined with client
    save gold parquet
'''
#%% 
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, ArrayType,StructType,StructField, LongType, BooleanType, TimestampType, DateType

def init():
    schema = StructType([
        StructField("mongo_id",StringType(),True),
        StructField("client_id",StringType(),True),
        StructField("client_name",StringType(),True),
        StructField("user_ID",StringType(),True),
        StructField("name",StringType(),True),
        StructField("channel",StringType(),True),
        StructField("services",StringType(),True),
        StructField("symptoms",ArrayType(StringType()),True),
        StructField("Quantos_dias_suspeitando_covid",LongType(),True),
        StructField("Teve_contato_com_alguem_de_covid",BooleanType(),True),
        StructField("diagnostic",StringType(),True),
        StructField("atendimento_humano",BooleanType(),True),
        StructField("nota_usuario",LongType(),True),
        StructField("created_at_utc",TimestampType(),True), 
    ])

    spark = (
        SparkSession.builder
        .appName("silverToGold")
        .getOrCreate()
    )

    df=(spark.read
        .schema(schema)
        .parquet("silver.parquet")
    )

    df.write.mode('append').parquet("gold.parquet")
    print("gold.parquet created successfully.")

if __name__ == "__main__":
    init()
