import pandas.io.data as web
import datetime

import financial

from pyspark import SparkContext

print("*** Ejercicio 1 ***")

start = datetime.datetime(2015, 1, 1)
end = datetime.datetime(2016, 7, 27)

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
    file_rdd = sc.textFile(file)

    split_rdd = financial.transform(file_rdd)
    print(split_rdd.take(5))

    filter_rdd = split_rdd.filter(lambda x: x.Date.year in [2015, 2016])
    # print(filter_RDD.take(5))
    return filter_rdd


msft_RDD = parse_and_filter("msft.csv")
print("MSFT", msft_RDD.take(5))

goog_RDD = parse_and_filter("goog.csv")
print("GOOG", goog_RDD.take(5))

aapl_RDD = parse_and_filter("aapl.csv")
print("AAPL", aapl_RDD.take(5))


print("MSFT", msft["2016-01-01":"2016-01-31"])
print("GOOG", goog["2016-01-01":"2016-01-31"])
print("AAPL", aapl["2016-01-01":"2016-01-31"])

