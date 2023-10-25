from pyspark import SparkContext


def parser(line):
    fields = line.split(",")
    sentence = fields[0]
    amount_spent = float(fields[10])

    return (amount_spent, sentence)


def load_boring_words():
    boring_words_file = open("E:/Big data course/Week-10/DataSets/boring_words.txt")

    lines = boring_words_file.readlines()

    boring_words = []
    for line in lines:

        if len(line) == 0:
            break
        else:
            boring_words.append(line.strip())

    return set(boring_words)


sc = SparkContext("local[*]", "amount spent on each word")

name_set = sc.broadcast(load_boring_words())

# Change the logging level to only display errors
sc.setLogLevel("ERROR")

# Reading the text file
input_rdd = sc.textFile("E:/Big data course/Week-10/DataSets/big_data_campaign_data.csv")

mapped_input = input_rdd.map(parser)

words = mapped_input.flatMapValues(lambda x: x.split(" "))

final_mapped = words.map(lambda x: (x[1].lower(), x[0]))

# Remove boring words
# If x[0] present in the broad case then eliminate it
filteredRdd = final_mapped.filter(lambda x : x[0] not in name_set.value)

total = filteredRdd.reduceByKey(lambda x, y: x + y)

sorted_result = total.sortBy(lambda x: x[1], False)

result = sorted_result.take(20)

for item in sorted_result.collect():
    print("--> ", item)
