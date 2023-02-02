
# Streaming de pipelines ETL utilizando PySpark

<div align="center">
  <img src="https://github.com/arturfc/data-streaming-queries/blob/main/docs/images/arquitecture.png"/>
</div>

#### Overview
Dados são extraídos em tempo real para um banco de dados não relacional (MongoDB), prosseguido de pipelines (camadas bronze, silver e gold) que vão realizar as seguintes funções:
- *Bronze*: ingestão de dados brutos oriundos do MongoDB;
- *Silver*: criação de SSOT, remoção de registros duplicados e testes de qualidade de dados;
- *Gold*: Schema enforcement.

O arquivo [DMLqueries.py](https://github.com/arturfc/data-streaming-queries/blob/main/DMLqueries.py) cria uma *streaming spark session* que irá produzir views atualizadas de dados oriuntos do gold.parquet. 

### Características dos dados de entrada

<div>
  <img src="https://github.com/arturfc/data-streaming-queries/blob/main/docs/images/datatype%20structure%20example.png"/>
</div>

O MongoDB é alimentado por dados do tipo *.json*, com os campos representados na imagem acima. Para melhor entendimento, os dados são gerados com base neste [fluxograma](https://github.com/arturfc/data-streaming-queries/blob/main/docs/images/fluxogram%20of%20data%20generated.pdf), que simula dados gerados através de um atendimento de robô virtual com foco em diagnóstico de COVID-19.

## Setup do ambiente

Neste projeto foi utilizado [MongoDB](https://www.mongodb.com/) na versão 1.34.1 e [Apache Spark](https://spark.apache.org/downloads.html) (Spark versão 3.3.1 e Hadoop versão 2.7).


