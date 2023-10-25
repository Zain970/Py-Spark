from pyspark import SparkContext


def blank_line_checker(line):
    if len(line) == 0:
        my_accum.add(1)


sc = SparkContext("local[*]", "Accumulator example")

# Change the logging level to only display errors
sc.setLogLevel("ERROR")

# Reading the text file
input_rdd = sc.textFile("E:/Big data course/Week-10/DataSets/sample_file.txt")

my_accum = sc.accumulator(0)

input_rdd.foreach(blank_line_checker)

print("result : ",my_accum.value)
