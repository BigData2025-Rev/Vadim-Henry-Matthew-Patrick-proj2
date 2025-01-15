#Uses spark's accumulator to generate sequential numbers for the order id

from datetime import date, datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
import pandas as pd
import numpy as np
from pyspark.sql import functions as sf
import collections

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

#Generate random numbers of a normal distribution and use them as indexes for a list
#mean: average , dev: standard deviation, n: number of rows, min:lowest random index
#max: highest random index, list: list of elements to pick from
def randomNormalIndexes(mean, dev, n, min, max, list):
    #create the normal distribution and make them into integers
    normal = np.random.default_rng(seed=9).normal(mean, dev, n).astype(int)
    indexes = np.clip(normal,a_min=min,a_max=max) #ensure the indexes are not out of range
    result = [] #Use the random indexes to make a list of random elements from the given list
    for i in indexes:
        try:
            result.append(list[i])
        except Exception as e: 
            print("Invalid index")
            print(e)
    return result

n = 100
random.seed(9) #Give random a seed so the results is the same every time

list, acc = accumulate(n)
#print(list)
custIds = randomIDs(n, 20)
#print(custIds)
prodIds = randomIDs(n, 10)
#print(prodIds)

countries = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
mean = (len(countries))/2-0.5
countryRandom = randomNormalIndexes(mean, mean/2, n, 0, len(countries)-1,countries)
#print(countryRandom)
print(collections.Counter(countryRandom))

data = {"order_id":list, "customer_id":custIds, "product_id":prodIds, "country":countryRandom} #Create a dictionary of the data
#data = {"order_id":list}
pdDf = pd.DataFrame(data) #Pass the dictionary to pandas
df = spark.createDataFrame(pdDf) #pass the pandas dataframe to spark
df.show()
spark.stop()