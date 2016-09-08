import pandas as pd
import pandas.io.data as web
from datetime import datetime

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

filter_RDD = split_RDD.map(lambda x : (datetime.strptime(x[0], "%Y-%m-%d").date(), x))\
    .filter(lambda x : x[0].year == 2015 or x[0].year == 2016)
print(filter_RDD.take(5))

quarter_close_RDD = filter_RDD.map(lambda x : ( str(x[0].year) +"-Q"+ str((x[0].month-1)//3), (float(x[1][4]), 1))).\
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).\
    map(lambda x: (x[0], x[1][0]/x[1][1]))
print(quarter_close_RDD.take(10))
