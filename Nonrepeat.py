#Uses spark's accumulator to generate sequential numbers for the order id

from datetime import date, datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
import pandas as pd
spark = SparkSession.builder.getOrCreate()


def nonRepeat(n):
    numbers = range(0,n)
    #print(random.sample(numbers,n))
    numList = random.sample(numbers,n)
    print("List length:" + str(len(numList)))
    numSet = set(random.sample(numbers,n))
    print("Set length:" + str(len(numSet)))
    print(numList)
    print(numSet)

#Generate sequential numbers using pyspark's accumulator
def accumulate(n):
    list = []

    acc = spark.sparkContext.accumulator(0)
    for i in range(n):
        list.append(acc.value)
        acc += 1
    return list, acc

list, acc = accumulate(20)
print(list)

data = {"order_id":list} #Create a dictionary of the data
pdDf = pd.DataFrame(data) #Pass the dictionary to pandas

df = spark.createDataFrame(pdDf) #pass the pandas dataframe to spark

df.show()

spark.stop()