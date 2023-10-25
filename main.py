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
input_rdd = sc.textFile("E:/Big data course/Week-10/DataSets/big_data_compaign.csv")
