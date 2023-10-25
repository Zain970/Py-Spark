from pyspark import SparkContext

sc = SparkContext("local[*]", "Parallelize a list")
sc.setLogLevel("ERROR")

rdd = sc.textFile("E:/Big data course/Week-10/DataSets/big_Log.txt")

mapped_rdd = rdd.map(lambda x: (x.split(":")[0], x.split(":")[1]))

grouped_rdd = mapped_rdd.groupByKey()

# result = grouped_rdd.map(lambda x: (x[0], x[1].size))

for item in grouped_rdd.collect():
    print(item[0]," : ", len(item[1]))
