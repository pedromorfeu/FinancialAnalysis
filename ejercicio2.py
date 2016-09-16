import pandas as pd
import pandas.io.data as web
import datetime

from financial import FinancialData

from pyspark import SparkContext

print("*** Ejercicio 2 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


file_RDD = sc.textFile("msft.csv")

split_RDD = file_RDD.filter(lambda x: not x.startswith("Date"))\
    .map(lambda x: x.strip().split(","))
print(split_RDD.take(5))

filter_RDD = split_RDD.map(lambda x: FinancialData(x[0], x[1], x[2], x[3], x[4], x[5], x[6]))\
    .filter(lambda x: x.Date == datetime.date(2016, 6, 15) or x.Date == datetime.date(2016, 6, 16))
print(filter_RDD.take(5))

return_value = filter_RDD.map(lambda x: x.Close).reduce(lambda x, y: y/x-1)
print("Return:", return_value)
