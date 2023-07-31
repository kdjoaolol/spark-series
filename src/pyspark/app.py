# importanto as bibliotecas do spark para nosso projetinho
# builder -> inicia a session

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# load data
df_device = spark.read.json("/home/kdjoaolol/spark-series/docs/files/device/*.json")

# schema
df_device.printSchema()

# columns 
print(df_device.columns)

# rows 
print(df_device.count())

# select columns
df_device.select("manufacturer", "model", "platform").show()
df_device.selectExpr("manufacturer", "model", "platform as type").show()

# filter 
df_device.filter(df_device.manufacturer == "Xiaomi").show()

# group by 
df_grouped_manufacturer = df_device.groupBy("manufacturer").count()
df_grouped_manufacturer.show()