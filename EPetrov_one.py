# импортируем нужные библиотеки
from pyspark.sql import SparkSession

# создание spark сессии
spark = SparkSession.builder.appName("education").getOrCreate()

# считываем и записываем датасет с категориями
df = spark.read.format("parquet").load("/data/stage/epetrov/transactions.parquet")
df.repartition(1).write.format("parquet").save("/data/stage/epetrov/transactions")
