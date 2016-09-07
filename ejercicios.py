import pandas as pd
import pandas.io.data as web
from datetime import datetime

from pyspark import SparkContext

start = datetime(2010, 1, 1)
end = datetime(2016, 12, 31)

# get web data
msft = web.DataReader("MSFT", "google", start, end)
print(msft.head())
print(msft.shape)

goog = web.DataReader("GOOG", "google", start, end)
print(goog.head())
print(goog.shape)

aapl = web.DataReader("AAPL", "google", start, end)
print(aapl.head())
print(aapl.shape)

# write to file
msft.to_csv(path_or_buf="msft.csv", sep=",")
goog.to_csv(path_or_buf="goog.csv", sep=",")
aapl.to_csv(path_or_buf="aapl.csv", sep=",")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


def parse_and_filter_file(file):
    file_RDD = sc.textFile(file)

    split_RDD = file_RDD.filter(lambda x : not x.startswith("Date"))\
        .map(lambda x : x.strip().split(","))
    # print(split_RDD.take(5))

    dates_RDD = split_RDD.map(lambda x : (datetime.strptime(x[0], "%Y-%m-%d"), x))\
        .filter(lambda x : x[0].date().year == 2015 or x[0].date().year == 2016)\
        .map(lambda x : x[1])
    # print(dates_RDD.take(5))
    return dates_RDD


msft_RDD = parse_and_filter_file("msft.csv")
print(msft_RDD.take(5))

goog_RDD = parse_and_filter_file("goog.csv")
print(goog_RDD.take(5))

aapl_RDD = parse_and_filter_file("aapl.csv")
print(aapl_RDD.take(5))