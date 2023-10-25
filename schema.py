from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType, StringType, StructField

my_conf = SparkConf()

my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

# infer the schema
# implicit (parquet , avro)
# explicit (manually define the schema)

ordersSchema = StructType([
    StructField("orderid", IntegerType()),
    StructField("orderdate", TimestampType()),
    StructField("customerid", IntegerType()),
    StructField("status", StringType()),

])

ordersDDL = "orderid Integer , orderdate Timestamp , customerid Integer , status String "

# df = spark.read.format("csv") \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .option("path", "E:/Big data course/Week-11/DataSets/orders.csv") \
#     .load()

df = spark.read.format("csv") \
    .option("header", True) \
    .schema(ordersDDL) \
    .option("path", "E:/Big data course/Week-11/DataSets/orders.csv") \
    .load()

df.show()
df.printSchema()

spark.stop()
