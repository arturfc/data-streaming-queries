# Índice 

* [Overview](#overview)
* [Características dos dados de entrada](#características-dos-dados-de-entrada)
* [Arquivo ETL](#arquivo-etl)
* [Arquivo DMLqueries](#arquivo-dmlqueries)
* [Setup do ambiente](#setup-do-ambiente)


# Streaming de pipelines ETL utilizando PySpark

<div align="center">
  <img src="https://github.com/arturfc/data-streaming-queries/blob/main/docs/images/arquitecture.png"/>
</div>

#### Overview
Dados são extraídos em tempo real para um banco de dados não relacional (MongoDB), prosseguido de pipelines (camadas bronze, silver e gold) que vão realizar as seguintes funções:
- [*Bronze*](https://github.com/arturfc/data-streaming-queries/blob/main/sourceToBronzeV2.py): ingestão de dados brutos (como eles são) oriundos do MongoDB;
- [*Silver*](https://github.com/arturfc/data-streaming-queries/blob/main/bronzeToSilver.py): criação de SSOT, remoção de registros duplicados e testes de qualidade de dados;
- [*Gold*](https://github.com/arturfc/data-streaming-queries/blob/main/silverToGold.py): Schema enforcement e adição de funções de agregações (se necessário).

O arquivo [DMLqueries.py](https://github.com/arturfc/data-streaming-queries/blob/main/DMLqueries.py) cria uma *streaming spark session* que irá produzir views atualizadas de dados oriuntos do repositório gold.parquet. 

### Características dos dados de entrada

<div>
  <img src="https://github.com/arturfc/data-streaming-queries/blob/main/docs/images/datatype%20structure%20example.png"/>
</div>

O MongoDB é alimentado por dados do tipo *.json*, com os campos representados na imagem acima. Para melhor entendimento, os dados são gerados com base neste [fluxograma](https://github.com/arturfc/data-streaming-queries/blob/main/docs/images/fluxogram%20of%20data%20generated.pdf), que simula dados gerados através de um atendimento de robô virtual com foco em diagnóstico de COVID-19. O arquivo [jsonGenerator.py](https://github.com/arturfc/data-streaming-queries/blob/main/databaseGenerator/jsonGenerator.py) é o responsável por popular estes dados de forma randomizada para dentro do banco de dados.

### Arquivo [ETL](https://github.com/arturfc/data-streaming-queries/blob/main/ETL.py)

Este arquivo é responsável por executar as três pipelines em sequência.

### Arquivo [DMLQueries](https://github.com/arturfc/data-streaming-queries/blob/main/DMLqueries.py)

Este arquivo foi executado em janela interativa (jupyter notebook em arquivo .py). Portanto, para notar as atualizações do *streaming spark session*, basta executar a célula da query de interesse novamente.

## Setup do ambiente

Neste projeto foi utilizado [MongoDB](https://www.mongodb.com/) na versão 1.34.1 e [Apache Spark](https://spark.apache.org/downloads.html) (Spark versão 3.3.1 e Hadoop versão 2.7).



