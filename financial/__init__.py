from datetime import datetime

# Date,Open,High,Low,Close,Volume,Adj Close
# 2010-01-04,30.620001000000002,31.1,30.59,30.950001,38409100,25.884104
# 2010-01-05,30.85,31.1,30.639999,30.959999,49749600,25.892466
# 2010-01-06,30.879998999999998,31.08,30.52,30.77,58182400,25.733566


class FinancialData:
    def __init__(self, Date, Open, High, Low, Close, Volume, AdjClose):
        self.Date = datetime.strptime(Date, "%Y-%m-%d").date()
        self.Open = float(Open)
        self.High = float(High)
        self.Low = float(Low)
        self.Close = float(Close)
        self.Volume = float(Volume)
        self.AdjClose = float(AdjClose)


