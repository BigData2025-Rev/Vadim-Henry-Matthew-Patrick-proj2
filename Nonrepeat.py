#Uses spark's accumulator to generate sequential numbers for the order id

from datetime import date, datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
import pandas as pd
from pyspark.sql import functions as sf
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

#Generate a random integer ID 
#This one allows for repeats to simulate the same customer making multiple orders
#or the same product being ordered by multiple customers
#n: number of rows, size: max id number
def randomIDs(n, size):
    list = []
    for i in range(n):
        #num = int(round(sf.rand(seed=se)*size,0))
        num = random.randint(0,size)
        list.append(num)
    return list

#Generate sequential numbers using pyspark's accumulator
#n: number of rows
def accumulate(n):
    list = []

    acc = spark.sparkContext.accumulator(0)
    for i in range(n):
        list.append(acc.value)
        acc += 1
    return list, acc

n = 100
random.seed(9) #Give random a seed so the results is the same every time

list, acc = accumulate(n)
#print(list)

custIds = randomIDs(n, 20)
#print(custIds)

prodIds = randomIDs(n, 10)
#print(prodIds)

data = {"order_id":list, "customer_id":custIds, "product_id":prodIds} #Create a dictionary of the data
#data = {"order_id":list}
pdDf = pd.DataFrame(data) #Pass the dictionary to pandas

df = spark.createDataFrame(pdDf) #pass the pandas dataframe to spark

df.show()

spark.stop()