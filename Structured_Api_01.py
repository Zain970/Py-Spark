from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()

my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "E:/Big data course/Week-11/DataSets/orders.csv") \
    .load()

df.show()
df.printSchema()

df.select("order_id", "order_status").show()

# How may orders are placed by each customer where customer_id is greater then 10000
df \
    .where("order_customer_id > 10000") \
    .select("order_id", "order_customer_id") \
    .groupby("order_customer_id") \
    .count()

spark.stop()
