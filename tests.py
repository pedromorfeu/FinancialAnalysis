from datetime import datetime

d = datetime.strptime("2010-05-26", "%Y-%m-%d")
print(d.date().year)