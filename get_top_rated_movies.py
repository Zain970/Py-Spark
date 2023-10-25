from pyspark import SparkContext


# Fetch all the movie names which has the ratings greater then 4.5 and have been given more then 1000 reviews
def parser(line):
    fields = line.split("::")

    movie_id = fields[1]
    rating_given = float(fields[2])

    return (movie_id, rating_given)


sc = SparkContext("local[*]", "code")

input_rdd = sc.textFile("E:/Big data course/Week-11/DataSets/ratings.txt")

mapped_rdd = input_rdd.map(parser)

final_mapped = mapped_rdd.mapValues(lambda x: (x, 1.0))

reduce_rdd = final_mapped.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Movies who have got more than 1000 ratings
filtered_rdd = reduce_rdd.filter(lambda x: x[1][0] > 1)

# Movies whose average ratings is greater then 4.5
final_rdd = filtered_rdd.mapValues(lambda x: x[0] / x[1]).filter(lambda x: x[1] > 4.5)


# Movies data
movies_rdd = sc.textFile("E:/Big data course/Week-11/DataSets/movies.txt")

movies_mapped_rdd = movies_rdd.map(lambda x: (x.split("::")[0], x.split("::")[1]))


joined_rdd = movies_mapped_rdd.join(final_rdd)

top_movies_rdd = joined_rdd.map(lambda x: x[1][0])

result = top_movies_rdd.collect()

for movie_name in result:
    print(movie_name)
