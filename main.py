from pyspark import SparkContext

sc = SparkContext("local[*]", "Salting")

input_rdd = sc.textFile("E:/Big data course/Week-12/DataSets/big_Log.txt")

result = input_rdd.take(20)

for item in result.collect():
    print("-> ", item)
