import pandas as pd
import pandas.io.data as web
from datetime import datetime

from pyspark import SparkContext

print("*** Ejercicio 2 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


file_RDD = sc.textFile("msft.csv")

split_RDD = file_RDD.filter(lambda x : not x.startswith("Date"))\
    .map(lambda x : x.strip().split(","))
print(split_RDD.take(5))

filter_RDD = split_RDD.map(lambda x : (datetime.strptime(x[0], "%Y-%m-%d").date(), x))\
    .filter(lambda x : x[0].year == 2016 and x[0].month == 6 and x[0].day in [15,16])\
    .map(lambda x : x[1])
print(filter_RDD.take(5))

close_list = filter_RDD.map(lambda x : float(x[4])).collect()
return_value = close_list[1] / close_list[0] - 1
print("Return:", return_value)

