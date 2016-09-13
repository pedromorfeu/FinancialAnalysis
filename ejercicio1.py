import pandas as pd
import pandas.io.data as web
from datetime import datetime

from pyspark import SparkContext

print("*** Ejercicio 1 ***")

start = datetime(2010, 1, 1)
end = datetime(2016, 12, 31)

# get web data
msft = web.DataReader("MSFT", "yahoo", start, end)
print("MSFT", msft.head())
# print(msft.shape)

goog = web.DataReader("GOOG", "yahoo", start, end)
print("GOOG", goog.head())
# print(goog.shape)

aapl = web.DataReader("AAPL", "yahoo", start, end)
print("AAPL", aapl.head())
# print(aapl.shape)


# write to file
msft.to_csv(path_or_buf="msft.csv", sep=",")
goog.to_csv(path_or_buf="goog.csv", sep=",")
aapl.to_csv(path_or_buf="aapl.csv", sep=",")


# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


# parse file and filter by years 2015 and 2016
def parse_and_filter(file):
    file_RDD = sc.textFile(file)

    split_RDD = file_RDD.filter(lambda x : not x.startswith("Date"))\
        .map(lambda x : x.strip().split(","))
    # print(split_RDD.take(5))

    filter_RDD = split_RDD.map(lambda x : (datetime.strptime(x[0], "%Y-%m-%d").date(), x))\
        .filter(lambda x : x[0].year == 2015 or x[0].year == 2016)\
        .map(lambda x : x[1])
    # print(filter_RDD.take(5))
    return filter_RDD


msft_RDD = parse_and_filter("msft.csv")
print("MSFT", msft_RDD.take(5))

goog_RDD = parse_and_filter("goog.csv")
print("GOOG", goog_RDD.take(5))

aapl_RDD = parse_and_filter("aapl.csv")
print("AAPL", aapl_RDD.take(5))


print("MSFT", msft["2016-01-01":"2016-01-31"])
print("GOOG", goog["2016-01-01":"2016-01-31"])
print("AAPL", aapl["2016-01-01":"2016-01-31"])

