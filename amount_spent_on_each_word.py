from pyspark import SparkContext


def parser(line):
    fields = line.split(",")
    sentence = fields[0]
    amount_spent = float(fields[10])

    return (amount_spent, sentence)


sc = SparkContext("local[*]", "amount spent on each word")

# Change the logging level to only display errors
sc.setLogLevel("ERROR")

# Reading the text file
input_rdd = sc.textFile("E:/Big data course/Week-10/DataSets/big_data_campaign_data.csv")

mapped_input = input_rdd.map(parser)

words = mapped_input.flatMapValues(lambda x: x.split(" "))

final_mapped = words.map(lambda x: (x[1].lower(), x[0]))

total = final_mapped.reduceByKey(lambda x, y: x + y)

sorted_result = total.sortBy(lambda x: x[1], False)

print("-> ", sorted_result.getNumPartitions())

sorted_result = sorted_result.repartition(8)
print("-> ", sorted_result.getNumPartitions())

# sorted_result.saveAsTextFile("C:/Users/zain9/OneDrive/Desktop/Spark-Python/Week-10/Output")

result = sorted_result.take(20)

for item in sorted_result.collect():
    print("--> ", item)
