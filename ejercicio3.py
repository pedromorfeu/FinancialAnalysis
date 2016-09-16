import financial

from pyspark import SparkContext


def mean(_list):
    result = sum(_list)/len(_list)
    return round(result, 6)


print("*** Ejercicio 3 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


file_rdd = sc.textFile("msft.csv")

split_rdd = financial.transform(file_rdd)
print(split_rdd.take(5))

filter_rdd = split_rdd.filter(lambda x: x.Date.year in [2015, 2016])
print(filter_rdd.take(5))

quarter_close_rdd = filter_rdd.map(lambda x: (str(x.Date.year) + "-Q" + str((x.Date.month - 1) // 3), x.Close))\
    .groupByKey()\
    .map(lambda x: (x[0], mean(list(x[1]))))
print(quarter_close_rdd.collect())

