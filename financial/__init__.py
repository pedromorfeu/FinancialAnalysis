from datetime import datetime

# Date,Open,High,Low,Close,Volume
# 2010-01-04,30.62,31.1,30.59,30.95,38414185
# 2010-01-05,30.85,31.1,30.64,30.96,49758862
# 2010-01-06,30.88,31.08,30.52,30.77,58182332

class FinancialData:
    def __init__(self, Date, Open, High, Low, Close, Volume):
        self.Date = datetime.strptime(Date, "%Y-%m-%d").date()
        self.Open = float(Open)
        self.High = float(High)
        self.Low = float(Low)
        self.Close = float(Close)
        self.Volume = float(Volume)
