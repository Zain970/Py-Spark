from pyspark import SparkContext

sc = SparkContext("local[*]", "Parallelize a list")
sc.setLogLevel("ERROR")

if __name__ == "__main__":
    my_list = [
        "WARN:Tuesday 4 September 0405",
        "ERROR:Tuesday 4 September 0408",
        "ERROR:Tuesday 4 September 0408",
        "ERROR:Tuesday 4 September 0408",
        "ERROR:Tuesday 4 September 0408",
        "ERROR:Tuesday 4 September 0408"]

    rdd = sc.parallelize(my_list)

else:
    rdd = sc.textFile("E:/Big data course/Week-10/DataSets/big_Log.txt")

mapped_rdd = rdd.map(lambda x: (x.split(":")[0], 1))

resultant_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)

result = resultant_rdd.collect()

for item in result:
    print("--> ", item)
