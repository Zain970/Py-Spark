from pyspark import SparkContext
from pyspark import StorageLevel


def parser(line):
    fields = line.split(",")

    customer_id = fields[0]
    amount_spent = float(fields[2])

    return (customer_id, amount_spent)


sc = SparkContext("local[*]", "code")

input_rdd = sc.textFile("E:/Big data course/Week-11/DataSets/customer-orders.csv")

mapped_rdd = input_rdd.map(parser)

total_by_customer = mapped_rdd.reduceByKey(lambda x, y: x + y)

premium_customers = total_by_customer.filter(lambda x: x[1] > 5000)

# Persist the rdd , so for the 2nd action not all the input is computed again
doubled_amount = premium_customers.map(lambda x: (x[0], x[1] * 2)).persist(StorageLevel.MEMORY_ONLY)

result = doubled_amount.collect()

for item in result:
    print("-> ", item)

print("Total Premium customers : ", doubled_amount.count())

# How to execute spark submit job in python
# spark-submit "C:/Users/zain9/OneDrive/Desktop/Spark-Python/Week-11/get_premium_customers.py"
