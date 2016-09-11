import pandas as pd
import pandas.io.data as web
from datetime import datetime

from financial import FinancialData

from pyspark import SparkContext

from operator import add

print("*** Ejercicio 3 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


file_RDD = sc.textFile("msft.csv")

split_RDD = file_RDD.filter(lambda x : not x.startswith("Date"))\
    .map(lambda x : x.strip().split(","))
print(split_RDD.take(5))

# Date,Open,High,Low,Close,Volume
filter_RDD = split_RDD.map(lambda x : FinancialData(x[0],x[1],x[2],x[3],x[4],x[5]))\
    .filter(lambda x : x.Date.year in [2015,2016])
print(filter_RDD.take(5))

quarter_close_RDD = filter_RDD.map(lambda x : ( str(x.Date.year) +"-Q"+ str((x.Date.month-1)//3), (x.Close, 1))).\
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).\
    map(lambda x: (x[0], x[1][0]/x[1][1]))
print(quarter_close_RDD.take(10))
