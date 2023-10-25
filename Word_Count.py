from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[*]", "wordcount")

sc.setLogLevel("ERROR")

input = sc.textFile("E:/Big data course/Week-09/DataSets/word_count_1.txt")

words = input.flatMap(lambda x: x.split(" "))

wordCounts = words.map(lambda x: (x.lower(), 1))

finalCount = wordCounts.reduceByKey(lambda x, y: x + y)

sortedResult = finalCount.sortBy(lambda x: x[1],False)

result = sortedResult.collect()

for a in result:
    print(a)

# To check the spark history server , so stopping the program termination
stdin.readline()
