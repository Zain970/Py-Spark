from sys import stdin

from pyspark import SparkContext

# Scala was connecting to the spark core directly
# Pyspark used api library
# Hence scala dag matches to our code but pysark dag does not.

# When running the file directly
if __name__ == "main":

    # Common lines
    sc = SparkContext("local[*]", "wordcount")

    # Change the logging level to only display errors
    sc.setLogLevel("ERROR")

    # Reading the text file
    input = sc.textFile("E:/Big data course/Week-09/DataSets/word_count_2.txt")

    words = input.flatMap(lambda x: x.split(" "))

    # Converting to lower case so that words like hadoop , Hadoop are not treated differently
    wordCounts = words.map(lambda x: (x.lower(), 1))

    finalCount = wordCounts.reduceByKey(lambda x, y: x + y)

    sortedResult = finalCount.sortBy(lambda x: x[1], False)

    result = sortedResult.collect()

    for a in result:
        print(a)

    # Holding the program
    stdin.readline()
else:
    print("Not executed directly")

    # Common lines
    sc = SparkContext("local[*]", "wordcount")

    # Change the logging level to only display errors
    sc.setLogLevel("ERROR")

    # Reading the text file
    input = sc.textFile("E:/Big data course/Week-09/DataSets/word_count_2.txt")

    words = input.flatMap(lambda x: x.split(" "))

    # Converting to lower case so that words like hadoop , Hadoop are not treated differently
    wordCounts = words.map(lambda x: (x.lower()))

    # Returns a dictionary with the word as the key and the value as the count of that word
    final_count = wordCounts.countByValue()

    print(final_count)


