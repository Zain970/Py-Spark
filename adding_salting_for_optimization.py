from pyspark import SparkContext
import random


def parser(line):
    fields = line.split(":")

    log = fields[0]
    time = fields[1]

    num = random.randint(1, 20)

    return (log + str(num), time)


def merge_same(x):
    if x[0][0:5] == "WARN":
        return ("WARN", x[1])
    else:
        return ("ERROR", x[1])


sc = SparkContext("local[*]", "Salting")

input_rdd = sc.textFile("E:/Big data course/Week-12/DataSets/big_Log.txt")

# Also perform salting
mapped_rdd = input_rdd.map(parser)

grouped_rdd = mapped_rdd.groupByKey()

rdd4 = grouped_rdd.map(lambda x: (x[0], len(x[1])))

rdd5 = rdd4.map(merge_same)

rdd6 = rdd5.reduceByKey(lambda x, y: x + y)

for item in rdd6.collect():
    print("--> ", item)
