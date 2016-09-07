import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME']="C:/BigData/Financial Analysis Ii/spark-1.6.2-bin-hadoop2.6/python"

# Append pyspark to Python Path
sys.path.append("C:/BigData/Financial Analysis Ii/spark-1.6.2-bin-hadoop2.6/python")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)