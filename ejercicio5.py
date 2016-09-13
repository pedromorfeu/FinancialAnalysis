import pandas as pd
import pandas.io.data as web
from datetime import datetime

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

filter_rdd = split_rdd.filter(lambda x: x.Date.year == 2015 and x.Date.month == 1 and x.Date.day == 2)
print(filter_rdd.collect())

print(filter_rdd.collect()[0].AdjClose)

prices = filter_rdd.collect()[0].AdjClose

shares = 1000
portfolio_value = shares * prices
print(portfolio_value)
