
# Streaming de pipelines ETL utilizando PySpark

<imagem aqui da arquitetura do projeto>

#### Overview
Dados são extraídos em tempo real para um banco de dados não relacional (MongoDB), prosseguido de pipelines (camadas bronze, silver e gold) que vão realizar as seguintes funções:
- *Bronze*: ingestão de dados brutos oriundos do MongoDB;
- *Silver*: criação de SSOT, remoção de registros duplicados e testes de qualidade de dados;
- *Gold*: Schema enforcement.

O arquivo <DMLqueries.py> cria uma *streaming spark session* que irá produzir views atualizadas de dados oriuntos do gold.parquet. 

### Características dos dados de entrada

<imagem aqui dos datatypes do mongoDB>

O MongoDB é alimentado por dados do tipo *.json*, com os campos representados na imagem <imagem>. Para melhor entendimento, os dados são gerados com base neste <fluxograma>, que simula dados gerados através de um atendimento de robô virtual com foco em diagnóstico de COVID-19.

## Setup do ambiente

Neste projeto foi utilizado [MongoDB](https://www.mongodb.com/) na versão 1.34.1 e [Apache Spark](https://spark.apache.org/downloads.html) (Spark versão 3.3.1 e Hadoop versão 2.7).
