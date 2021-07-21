# Scala-ETL-Example

O projeto visa simular o processo de uma ETL bem simplificadamente. A escolha de usar Scala e Spark é a alta performance com o tratamento de dados em um ambiente de big data, e dessa forma podemos trazer mais valor agregado aos jobs que rodam em nuvem e também mais eficiência na geração dos dados e suas pipelines.

## Build

Você precisará ter [SBT](https://www.scala-sbt.org/download.html), [Scala](https://www.scala-lang.org/) 2.12 e [Apache Spark](https://spark.apache.org/) 2.x instalados na sua máquina, caso não tenha, baixar nos links abaixo: 
- https://www.scala-lang.org/download/
- https://spark.apache.org/downloads.html
- https://www.scala-sbt.org/download.html

Para rodar localmente, você deve usar os comandos abaixo:

```bash
sbt clean assembly
```

```bash
spark-submit target/scala-2.12/nestle_2.12-1.0.jar --packages com.crealytics:spark-excel_2.12:0.13.7 --class=etl.NestleApp
```
