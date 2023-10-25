from pyspark import SparkContext


def parser(line):
    fields = line.split("\t")
    rating = fields[2]

    return (rating, 1)


# Common lines
sc = SparkContext("local[*]", "wordcount")

# Change the logging level to only display errors
sc.setLogLevel("ERROR")

# Reading the text file
input_rdd = sc.textFile("E:/Big data course/Week-09/DataSets/movie-data.txt")

mapped_Rdd = input_rdd.map(parser)

result = mapped_Rdd.reduceByKey(lambda x, y: x + y)

sorted_result = result.sortBy(lambda x: x[1], False)

for item in sorted_result.collect():
    print(item)
