import pandas as pd
import pandas.io.data as web
import datetime

from financial import FinancialData

from pyspark import SparkContext

print("*** Ejercicio 5 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)

file_rdd = sc.textFile("aapl.csv")

split_rdd = file_rdd.filter(lambda x: not x.startswith("Date")) \
    .map(lambda x: x.strip().split(",")) \
    .map(lambda x: FinancialData(x[0], x[1], x[2], x[3], x[4], x[5], x[6]))
print(split_rdd.take(5))

# filter_rdd = split_rdd.filter(lambda x: x.Date.year == 2015 and x.Date.month == 1 and x.Date.day == 2)
# print(filter_rdd.collect()[0].AdjClose)

prices = split_rdd.map(lambda x: (x.Date, x.AdjClose))
print(prices.take(5))

prices.cache()

shares = 1000

portfolio_value = prices.filter(lambda x: x[0] == datetime.date(2015, 1, 2))\
    .map(lambda x: x[1])\
    .reduce(lambda x, y: shares * x + y)
print(portfolio_value)


print(prices.take(5))

prices_1 = prices.map(lambda x: (x[0] - datetime.timedelta(days=1), x[1]))
print(prices_1.take(5))

prices_all = sc.union([prices, prices_1])
# prices = prices_all.groupByKey()


def div(t_1, t):
    return t/t_1

prices = prices_all.reduceByKey(lambda x, y: div(x, y))
print(prices.takeOrdered(5))

# prices.map(lambda x: x[1])
