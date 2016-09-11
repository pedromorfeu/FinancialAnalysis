from datetime import datetime

from pyspark import SparkContext


f = float(12.33)
print(f)

exit()

d = datetime.strptime("2010-05-26", "%Y-%m-%d")
print(d.date().year)

sc = SparkContext("local", "Simple App")
print(sc)

list = sc.parallelize(["aaa,bbb,122","ccc,ddd,45.5"])

result = list.map(lambda x : x.strip().split(","))\
    .map(lambda x : (x[0],x[1],float(x[2])))\
    .map(lambda x : x[2])\
    .collect()

print(result)

class MyValues:
    def __init__(self, a, b, c):
        self.a = a
        self.b = b
        self.c = c

result1 = list.map(lambda x : x.strip().split(","))\
    .map(lambda x : MyValues(x[0],x[1],float(x[2])))\
    .map(lambda x : x.c)\
    .collect()

print(result1)
