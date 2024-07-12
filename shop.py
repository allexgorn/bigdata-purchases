from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DateType, StructType, StructField
from random import *
from datetime import *

category = ["Телевизор", "Компьютер", "Ноутбук", "Видеокамера", "Стереосистема"]
brands = ["Samsung", "Honor", "Apple", "Lenovo", "Xiaomi", "Huawei"]
prefix_models = ["ML","MS","DL","NT","WF","PR"]

purchases = []
for i in range(1,5001):
    datestamp = date(2024, randint(1,7), randint(1,13))
    user_id = randint(1,3000001)
    product = f'{choice(category)} {choice(brands)} {choice(prefix_models)}-{randint(1000,500000)}'
    count = randint(1,500)
    price = randint(10000,80000)
    purchases.append((datestamp, user_id, product, count, price))

spark = SparkSession.builder.appName("Test").getOrCreate()

purchaseSchema = StructType([
    StructField("Date", DateType(), False),
    StructField("UserID", IntegerType(), False),
    StructField("Product", StringType(), False),
    StructField("Count", IntegerType(), False),
    StructField("Price", IntegerType(), False)
])

purchaseFrame = spark.createDataFrame(purchases, purchaseSchema)
purchaseFrame.show()

purchaseFrame.write.csv("purchases", mode="overwrite",header=True)

spark.stop()