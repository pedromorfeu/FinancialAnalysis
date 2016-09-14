import pandas as pd
import pandas.io.data as web
import datetime

from financial import FinancialData

from pyspark import SparkContext

print("*** Ejercicio 4 ***")

# create Spark context
sc = SparkContext("local", "Simple App")
print(sc)


def calculate_vt(file):
    file_rdd = sc.textFile(file)

    split_rdd = file_rdd.filter(lambda x: not x.startswith("Date"))\
        .map(lambda x: x.strip().split(","))\
        .map(lambda x: FinancialData(x[0], x[1], x[2], x[3], x[4], x[5], x[6]))
    print(split_rdd.take(5))

    filter_rdd = split_rdd.filter(lambda x: (x.Date.year == 2016 and x.Date.month == 3 and x.Date.day == 30) or
                                            (x.Date.year == 2016 and x.Date.month == 4 and x.Date.day == 29))
    # print(filter_RDD.collect())
    # vts = filter_rdd.collect()
    # vt_1 = vts[0]
    # vt = vts[1]
    # print("Vt-1 date", vt_1.Date)
    # return vt_1, vt
    # print("Vt date", vt.Date)
    return filter_rdd


msft_filter = calculate_vt("msft.csv")
aapl_filter = calculate_vt("aapl.csv")

msft_filter.cache()
aapl_filter.cache()

msft_vt_1 = msft_filter.filter(lambda x: x.Date.day == 30).map(lambda x: x.Close)
aapl_vt_1 = aapl_filter.filter(lambda x: x.Date.day == 30).map(lambda x: x.Close)
msft_vt = msft_filter.filter(lambda x: x.Date.day == 29).map(lambda x: x.Close)
aapl_vt = aapl_filter.filter(lambda x: x.Date.day == 29).map(lambda x: x.Close)
print(msft_vt_1.take(1))
print(msft_vt.take(1))
print(aapl_vt_1.take(1))
print(aapl_vt.take(1))

msft_shares = 10
aapl_shares = 10
msft_aapl = sc.union([msft_vt_1, aapl_vt_1])

initial_portfolio_value = msft_aapl.reduce(lambda x, y: (msft_shares * x) + (aapl_shares * y))
print(initial_portfolio_value)


x_msft = msft_vt_1.map(lambda x: (msft_shares * x)/initial_portfolio_value)
x_aapl = aapl_vt_1.map(lambda x: (aapl_shares * x)/initial_portfolio_value)

print("x_msft", x_msft.take(1))
print("x_aapl", x_aapl.take(1))
print(msft_aapl.reduce(lambda x, y: ((msft_shares * x)/initial_portfolio_value) +
                                    ((aapl_shares * y)/initial_portfolio_value)))


msft = sc.union([msft_vt, msft_vt_1])
aapl = sc.union([aapl_vt, aapl_vt_1])

ret_msft = msft.reduce(lambda x, y: x/y-1)
ret_aapl = aapl.reduce(lambda x, y: x/y-1)
print(ret_msft)
print(ret_aapl)


msft_aapl = sc.union([x_msft, x_aapl])

rpt = msft_aapl.reduce(lambda x, y: (x * ret_msft) + (y * ret_aapl))
print(rpt)

vt = initial_portfolio_value * (1 + rpt)
print(vt)
