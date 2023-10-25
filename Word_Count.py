from pyspark import SparkContext
from sys import stdin

# When running the file directly
if __name__ == "main":

    # Common lines
    sc = SparkContext("local[*]", "wordcount")

    # Change the logging level to only display errors
    sc.setLogLevel("ERROR")

    # Reading the text file
    input = sc.textFile("E:/Big data course/Week-09/DataSets/word_count_1.txt")

    words = input.flatMap(lambda x: x.split(" "))

    wordCounts = words.map(lambda x: (x.lower(), 1))

    finalCount = wordCounts.reduceByKey(lambda x, y: x + y)

    sortedResult = finalCount.sortBy(lambda x: x[1], False)

    result = sortedResult.collect()

    for a in result:
        print(a)

    stdin.readline()
else:
    print("Not executed directly")
