import pandas as pd
import pandas.io.data as web
from datetime import datetime

msft = web.DataReader("MSFT", "google", datetime(2015, 12, 25), datetime(2016, 12, 31))
print(msft.head())

goog = web.DataReader("GOOG", "google", datetime(2015, 12, 25), datetime(2016, 12, 31))
print(goog.head())

aapl = web.DataReader("AAPL", "google", datetime(2015, 12, 25), datetime(2016, 12, 31))
print(aapl.head())
