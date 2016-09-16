import pandas as pd
import pandas.io.data as web
import datetime

import math

import financial

from pyspark import SparkContext

print("*** Ejercicio 5 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)

file_rdd = sc.textFile("aapl.csv")

split_rdd = financial.transform(file_rdd)
print(split_rdd.take(5))

prices = split_rdd.map(lambda x: (x.Date, x.AdjClose))
print(prices.take(5))

prices.cache()

shares = 1000

portfolio_value = prices.filter(lambda x: x[0] == datetime.date(2015, 1, 2))\
    .map(lambda x: x[1] * shares)\
    .reduce(lambda x, y: x+y)
print(portfolio_value)


print("prices", prices.take(5))


prices_index = prices.map(lambda x: x[1])\
    .zipWithIndex()\
    .map(lambda x: (x[1], x[0]))
print("prices_index", prices_index.take(5))
print("prices_index", prices_index.count())

prices_1 = prices_index.map(lambda x: (x[0]+1, x[1]))
# prices_1 = prices.map(lambda x: (x[0] - datetime.timedelta(days=1), x[1]))
print("prices_1", prices_1.take(5))
print("prices_1", prices_1.count())

prices_all = sc.union([prices_index, prices_1])
# prices = prices_all.groupByKey()


def div(x, y):
    # return math.log(x/y)
    return x / y

min_index = prices_all.min()[0]
print("min", min_index)
max_index = prices_all.max()[0]
print("max", max_index)

prices_reduce = prices_all.filter(lambda x: x[0] != min_index and x[0] != max_index)\
    .reduceByKey(lambda x, y: div(x, y))
# prices = prices_all.reduceByKey(lambda x, y: div(x, y))\
#     .filter(lambda x: x[0] != datetime.date(2015, 1, 1))\
#     .map(lambda x: x[1])
print("prices new", prices_reduce.takeOrdered(5))
print("prices", prices_reduce.count())

prices_reduce = prices_reduce.map(lambda x: (x[0], math.log(x[1])))
print("prices new log", prices_reduce.takeOrdered(5))


prices_std = prices_reduce.map(lambda x: x[1]).stdev()
print("std", prices_std)
# z_score = norm.ppf(0.95) # loc:0 (μ), scale: 1 (σ)
z_score = 1.6448536269514722
print("z_score", z_score)
alpha = z_score * prices_std
print("alpha", alpha)

value_at_risk = portfolio_value * alpha
print(value_at_risk)
