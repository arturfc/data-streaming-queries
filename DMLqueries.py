#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode
from pyspark.sql.functions import lit

spark = (
    SparkSession.builder
    .appName("DMLqueries")
    .getOrCreate()
)

df=(spark.read
    .parquet("gold.parquet")
)

#df.createOrReplaceTempView('gold_table')

schema = df.schema
streaming = (
    spark.readStream.schema(schema)
    #.option("maxFilesPerTrigger", 1)
    .parquet("gold.parquet/")
)

streaming.createOrReplaceTempView('gold_table')

query = spark.sql(''' 
    SELECT * from gold_table
''')

activityQuery = (
    query.writeStream.queryName("gold_table")
    .outputMode("append")
    .format("memory")
    .start()
)

#%%
#checking streaming spark session status
print(f"Is stream active: {activityQuery.isActive}")
print(activityQuery.status)

#%%
#Exibindo número total de documentos
spark.sql(''' 
    SELECT count(*) as total_docs
    from gold_table
''').show()

#%%
#Exibindo o último usuário atendido
spark.sql(''' 
    SELECT name, substr(created_at_utc,11) as period
    from gold_table
    group by name, created_at_utc
    order by created_at_utc desc
    limit 1
''').show()

#%%
#Quantos foram os atendimentos por cliente? 
spark.sql(''' 
    SELECT client_id, client_name, count(*) qtd_atendimentos
    FROM gold_table
    GROUP BY client_id, client_name 
''').show()

#%%
#Quais clientes possuem mais atendimentos?

spark.sql(''' 
    SELECT client_id, client_name, count(*) qtd_atendimentos
    FROM gold_table
    GROUP BY client_id, client_name
    ORDER BY qtd_atendimentos desc
    LIMIT 3
''').show()

#%%
#Quantos atendimentos foram em qual canal (wpp, telegram, site)

spark.sql(''' 
    SELECT channel, count(*) qtd
    FROM gold_table
    GROUP BY channel
    ORDER BY qtd desc
''').show()

#%%
#Quais serviços foram fornecidos em cada atendimento?

spark.sql(''' 
    SELECT DISTINCT mongo_id, name, services
    FROM gold_table
    ORDER BY mongo_id, name, services
''').show()

#%%
#Quais serviços são mais acessados?

spark.sql(''' 
    SELECT services, count(*) qtd
    FROM gold_table
    GROUP BY services
    ORDER BY qtd desc
''').show()

#%%
#Quantos atendimentos são de cada serviço?

spark.sql(''' 
    SELECT services, count(*) qtd
    FROM gold_table
    GROUP BY services
''').show()

#%%
#Os atendimentos estão aumentando ou diminuindo no decorrer do tempo?

spark.sql(''' 
    SELECT substr(created_at_utc,1,7) as period, count(*) as qtd_atendimentos
    FROM gold_table
    GROUP BY period
    ORDER BY period
''').show()

#%%
#Quantos usuários responderam se sentem dor de cabeça frequentemente?
#E outros sintomas de doenças?
#ex: cliente possui id 9yuroy1XaHlD

df_d = (df.select("*", explode("symptoms").alias("exploded"))
    .where((col("exploded") == "dor_de_cabeca") & (col("client_name") == 'SUS'))
    .agg(count("exploded").alias("dor_de_cabeca")))

df_o = (df.select("*", explode("symptoms").alias("exploded"))
    .where((col("exploded") != "dor_de_cabeca") & (col("client_name") == 'SUS'))
    .agg(count("exploded").alias("outras")))

df_d.withColumn("outras", lit(df_o.select(df_o.outras).collect()[0][0])) \
  .show()

#%%
#Qual a média de notas que o usuário está dando para o atendimento?
#ex: cliente possui id 9yuroy1XaHlD

spark.sql('''
    SELECT client_id, avg(nota_usuario) as nota_media
    FROM gold_table
    WHERE client_id = '9yuroy1XaHlD'
    GROUP BY client_id
''').show()

#%%
#Quantos usuários responderam que tiveram contato com alguem diagnosticado com covid?
#ex: cliente possui id LPwtuW9Uk5lO
spark.sql(''' 
    SELECT client_id, client_name, sum(decode(Teve_contato_com_alguem_de_covid, True, 1, 0)) as yes,
           sum(decode(Teve_contato_com_alguem_de_covid, False, 1, 0)) as no
    FROM gold_table
    WHERE client_id = 'LPwtuW9Uk5lO'
    GROUP BY client_id, client_name
''').show()

#%%
#Qual foi o desfecho dos atendimentos?
#Quantos foram encaminhados para um atendimento humano?
#Quantos foram concluídos dentro do atendimento eletrônico?
#ex: cliente possui id 9yuroy1XaHlD

#MUDAr DPS para atendimento_humano
spark.sql(''' 
    SELECT client_id, client_name, sum(decode(atendimento_humano, True, 1, 0)) as yes,
           sum(decode(atendimento_humano, False, 1, 0)) as no
    FROM gold_table
    WHERE client_id = '9yuroy1XaHlD'
    GROUP BY client_id, client_name
''').show()

# %%

'''
    streaming...
'''
schema = df.schema
streaming = (
    spark.readStream.schema(schema)
    #.option("maxFilesPerTrigger", 1)
    .parquet("gold.parquet/")
)

streaming.createOrReplaceTempView('streaming_table')

query = spark.sql(''' 
    SELECT client_id, client_name, count(*) qtd_atendimentos
    FROM streaming_table
    GROUP BY client_id, client_name
    ORDER BY qtd_atendimentos desc
    LIMIT 3
''')

activityQuery = (
    query.writeStream.queryName("streaming_table")
    .format("memory")
    .outputMode("complete")
    .start()
)
#%%
spark.sql("SELECT * FROM streaming_table").show()
#%%
