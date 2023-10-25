from pyspark import SparkContext


def parser(line):
    fields = line.split("::")
    age = int(fields[2])
    no_of_friends = int(fields[3])

    return (age, (no_of_friends, 1))


sc = SparkContext("local[*]", "wordcount")

# Change the logging level to only display errors
sc.setLogLevel("ERROR")

# Reading the text file
input_rdd = sc.textFile("E:/Big data course/Week-09/DataSets/friends-data.csv")

mapped_rdd = input_rdd.map(parser)

totals_By_Age = mapped_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

average_By_Age = totals_By_Age.mapValues(lambda x: (x[0] / x[1]))

result = average_By_Age.collect()

for item in result:
    print("--> ",item)