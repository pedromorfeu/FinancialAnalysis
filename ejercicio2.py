import pandas as pd
import pandas.io.data as web
from datetime import datetime

from financial import FinancialData

from pyspark import SparkContext

print("*** Ejercicio 2 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


file_RDD = sc.textFile("msft.csv")

split_RDD = file_RDD.filter(lambda x : not x.startswith("Date"))\
    .map(lambda x : x.strip().split(","))
print(split_RDD.take(5))

# Date,Open,High,Low,Close,Volume
filter_RDD = split_RDD.map(lambda x : FinancialData(x[0],x[1],x[2],x[3],x[4],x[5]))\
    .filter(lambda x : x.Date.year == 2016 and x.Date.month == 6 and x.Date.day in [15,16])
print(filter_RDD.take(5))

close_list = filter_RDD.map(lambda x : x.Close).collect()
return_value = close_list[1] / close_list[0] - 1
print("Return:", return_value)
