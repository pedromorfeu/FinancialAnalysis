import datetime

import financial

from pyspark import SparkContext

print("*** Ejercicio 2 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


file_rdd = sc.textFile("msft.csv")

split_rdd = financial.transform(file_rdd)
print(split_rdd.take(5))

filter_rdd = split_rdd.filter(lambda x: x.Date == datetime.date(2016, 6, 15) or x.Date == datetime.date(2016, 6, 16))
print(filter_rdd.take(5))

return_value = filter_rdd.map(lambda x: x.Close).reduce(lambda x, y: y / x - 1)
print("Return:", return_value)
