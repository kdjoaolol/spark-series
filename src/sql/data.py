# init & instantiate spark session
from pprint import pprint
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# data import
spark.sql("""
    CREATE TEMPORARY VIEW vw_device
    USING org.apache.spark.sql.json
    OPTIONS (path "/home/kdjoaolol/spark-series/docs/files/device/*.json")
""")

spark.sql("""
CREATE TEMPORARY VIEW vw_subscription
USING org.apache.spark.sql.json
OPTIONS (path "/home/kdjoaolol/spark-series/docs/files/subscription/*.json")
""")

pprint(spark.catalog.listTables())

# select data
spark.sql("""SELECT * FROM vw_device LIMIT 10;""").show()
spark.sql("""SELECT * FROM vw_subscription LIMIT 10;""").show()

# new dataframe = {join}
join_datasets = spark.sql("""
    SELECT dev.user_id,
           dev.model,
           dev.platform,
           subs.payment_method,
           subs.plan
    FROM vw_device AS dev
    INNER JOIN vw_subscription AS subs
    ON dev.user_id = subs.user_id
""")

# info
join_datasets.show()
join_datasets.printSchema()
pprint(join_datasets.count())