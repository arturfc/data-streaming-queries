'''
    read bronze json file
    modeling into a more optimized table and creating data quality checks
    save data into silver.parquet
'''
#%% 
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, ArrayType,StructType,StructField, LongType, BooleanType, TimestampType, DateType
from pyspark.sql.utils import AnalysisException
from great_expectations.dataset import SparkDFDataset
from datetime import datetime
from pyspark.sql.functions import from_utc_timestamp, col

def dataTransformation(df):   
    '''
        this function explodes the struct type column value
        parameters:
        -------------
        df: pyspark.sql.dataframe.DataFrame

        return: pyspark.sql.dataframe.DataFrame
    '''
    df_processed = (df
                    .withColumn('created_at_utc', from_utc_timestamp(col("created_at.$date"),"Europe/Moscow"))
                    .withColumn('mongo_id', (df['_id.$oid']))
                    .drop('_id')
                    .drop('created_at')
                    )
    return df_processed

def init():
    #creating spark session
    spark = (
        SparkSession.builder
        .appName("silver")
        .getOrCreate()
    )

    df = (spark.read
        .option("multiline","true")
        .json("bronze.json")
    )

    df = dataTransformation(df)

    #Getting the cols i want to work with 
    df = df.select(
        df.mongo_id,
        df.client_id,
        df.client_name, 
        df.user_ID,
        df.name,
        df.channel,
        df.services,
        df.symptoms,
        df.questions.Quantos_dias_suspeitando_covid[1].alias("Quantos_dias_suspeitando_covid"),
        df.questions.Teve_contato_com_alguem_de_covid[0].alias("Teve_contato_com_alguem_de_covid"),
        df.diagnostic,
        df.atendimento_humano,
        df.nota_usuario,
        df.created_at_utc
    )


    #Data quality unit tests
    MANDATORY_COLUMNS = [
        "mongo_id",
        "channel",
        "client_id",
        "client_name",
        "created_at_utc",
        "name",
        "diagnostic",
        "nota_usuario",
        "Teve_contato_com_alguem_de_covid",
        "Quantos_dias_suspeitando_covid",
        "services",
        "symptoms",
        "atendimento_humano",
        "user_ID"
    ]

    raw_df_test = SparkDFDataset(df)

    #check for mandatory columns
    for column in MANDATORY_COLUMNS:
        try:
            assert raw_df_test.expect_column_to_exist(column).success, f"Mandatory column {column} doest not exist: FAILED"
            print(f"Column {column} exists: PASSED")
        except AssertionError as e:
            print(e)

    #Mandatory columns should not be null
    for column in MANDATORY_COLUMNS:
        try:
            test_result = raw_df_test.expect_column_values_to_not_be_null(column)
            assert test_result.success, \
                print(f"{test_result.result['unexpected_count']} of {test_result.result['element_count']} items in colum {column} are null: FAILED")
            print(f"All items in colum {column} are not null: PASSED")
        except AssertionError as e:
            print(e)
        except AnalysisException as e:
            print(e)


    #Check for valid date format
    test_result = raw_df_test.expect_column_values_to_match_regex('created_at_utc', '\d{4}-\d{2}-\d{2}\s\d{2}(:)\d{2}(:)\d{2}')
    print(f"""{round(test_result.result['unexpected_percent'], 2)}% is not a valid date time format""")


    #check if mongo_id is unique
    test_result = raw_df_test.expect_column_values_to_be_unique("mongo_id")
    failed_msg = " ".join([
        f"""{test_result.result['unexpected_count']} of {test_result.result['element_count']} items""",
        f"""or {round(test_result.result['unexpected_percent'],2)}% are not unique: FAILED"""
    ])
    print(f"""{'Column mongo_id is unique: PASSED' if test_result.success else failed_msg}""")

    #creating silver.parquet
    df.write.mode('overwrite').parquet("silver.parquet")
    print("silver.parquet created successfully.")

if __name__ == "__main__":
    init()