#Call makeDataFrame for a spark dataframe containing the generated data with these columns in this order:
#order_id, customer_id, customer_name, product_id, product_name, product_category, price, country, city, ecommerce_website_name
#Uses dataGrabber.py, Camping-Products.csv, filtered-cities.csv, filtered-first-names, and filtered-last-names from the vadim branch
from datetime import date, datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
import pandas as pd
import numpy as np
from pyspark.sql import functions as sf
import collections
import dataGrabber #Make sure the latest version has been copied over from the vadim branch

#10 digit numbers to increase the id values for the final dataset so they aren't single digit
customerIdBase = 5300586482
orderIdBase = 4454673214
productIdBase = 1708861888

n = 15000
shape = 1 ; scale = 4
countries = ["United States", "Australia", "France", "Germany", "Switzerland", "Spain", "Portugal", "United Kingdom", "Ireland", "Italy", "Poland", "Belgium", "Netherlands", "Austria", "New Zealand", "Czechia", "Hungary", "Ukraine", "Russian Federation", "Romania", "Japan"]
repeatProb = 0.1
seed = 446

random.seed(9) #Give random a seed so the results is the same every time
rng = np.random.default_rng(seed=9)

#Generate sequential numbers using pyspark's accumulator
#n: number of rows, base:increase the sequential numbers by this value
def accumulate(n, base, spark):
    list = []

    acc = spark.sparkContext.accumulator(0)
    for i in range(n):
        list.append(acc.value + base)
        acc += 1
    return list, acc

#Generate random numbers of a normal distribution and use them as indexes for a list
#the first index is most likely and each index after becoming increasingly less likely
#mean: average , dev: standard deviation, n: number of rows, min:lowest random index
#max: highest random index, list: list of elements to pick from
def randomNormalIndexes(mean, dev, n, min, max): #, list
    #create the normal distribution and make them into integers
    normal = rng.normal(mean, dev, n).astype(int)
    normal = [abs(norm) for norm in normal]
    #normal = normal
    indexes = np.clip(normal,a_min=min,a_max=max) #ensure the indexes are not out of range
    return indexes

#With the gamma distribution using a shape of 1, the first index is most likely and
# each index after becoming increasingly less likely
def randomGammaIndexes(shape,scale,n, min, max): #,list
    gamma = rng.gamma(shape, scale, n).astype(int)
    indexes = np.clip(gamma,a_min=min,a_max=max) #ensure the indexes are not out of range
    return indexes

def randomChoiceIndexes(n, min, max, probs):
    range = np.arange(min, max+1, dtype=int)#max+1 since it is [min,max)
    #print("Probs:" + str(len(probs)))
    #print("range:" + str(len(range)))
    indexes = rng.choice(a=range,size=n,p=probs)
    return indexes

#Creates the 7,500 unique customers
#n: number of customers
def customerList(n, countries, shape, scale, min, max, spark, randSeed):
    #Possible id values range from min to max-1
    ids = rng.choice(a=spark.sparkContext.range(min,max).collect(),size=n,replace=False)
    d = 15000 #divisor
    probs = [2892/d,1855/d, 1590/d, 1364/d, 1124/d, 1003/d, 927/d, 878/d, 745/d, 621/d, 437/d, 399/d, 286/d, 225/d, 212/d, 206/d, 67/d, 57/d, 47/d, 39/d, 26/d]
    countryIndexes = randomChoiceIndexes(n, 0, len(countries)-1, probs)
    #countryIndexes = randomGammaIndexes(shape, scale, n, 0, len(countries)-1) #Pickout countries with a gamma distribution

    result = []
    for i in range(n):
        id = ids[i] #Get the next customer id from the generated non-repeating id list

        #Get the next country from the generated gamma distributed countries
        country = countries[countryIndexes[i]]  #The USA is most likely, then Australia, then New Zealand, etc.

        #Get a first and last name according to the country
        firstName = dataGrabber.get_data_by_country(country, "filtered-first-names.csv", randSeed)
        lastName = dataGrabber.get_data_by_country(country, 'filtered-last-names.csv', randSeed)
        city = dataGrabber.get_data_by_country(country, "filtered-cities.csv", randSeed)
        randSeed += 1 #Without this, the same country gives the same name everytime, since dataGrabber is not being used as a class instance
        name = firstName + " " + lastName
        customer = [id,name,country, city]
        result.append(customer)
    return result, randSeed

#Makes a 15,000 list of products
def productList(n):
    productIndexes = randomNormalIndexes(1, 96/3, n, 1, 96)
    ids = []
    name = []
    category = []
    price = []
    web = []
    for i in productIndexes:
        product = dataGrabber.get_product("Camping-Products.csv",i)
        ids.append(i + productIdBase) #Raise the id by productIdBase so it isn't a single digit number
        name.append(product[0])
        category.append(product[1])
        price.append(product[2])
        web.append(product[3])
    return ids, name, category, price, web

#Use the lists of repeat and onetime customers to create the final 15k lists of
# customer id, name, and country for the dataset
#repeatChance: float < 1 and is the chance of a repeat customer in the later half of the list
def finalLists(n1, n2, repeat, onetime, repeatChance, spark):
    ids = []
    names = []
    countries = []
    cities = []
    for i in spark.sparkContext.range(n1).collect():#The first half will be each of the repeat customers
        ids.append(repeat[i][0])
        names.append(repeat[i][1])
        countries.append(repeat[i][2])
        cities.append(repeat[i][3])
    chances = rng.random(n2) #Generate the random numbers for the repeat checks
    acc = spark.sparkContext.accumulator(0)#Used to get rows from the onetime customer list
    for i in spark.sparkContext.range(n2).collect():
        if repeatChance > chances[i]:#Check if it is randomly a repeat customer
            index = random.randint(0,len(repeat)-1)
            ids.append(repeat[index][0])
            names.append(repeat[index][1])
            countries.append(repeat[index][2])
            cities.append(repeat[index][3])
        else:
            index = acc.value #Use the next onetime customer
            ids.append(onetime[index][0])
            names.append(onetime[index][1])
            countries.append(onetime[index][2])
            cities.append(repeat[index][3])
            acc += 1
    return ids, names, countries, cities

#Creates the data frame for the dataset with order ids, customer ids, customer names, and countries
def makeDataFrame(spark):

    #Create order ids
    orderIds, acc = accumulate(n, orderIdBase, spark)
    nHalf = int(n/2)
    randSeed = seed
    #Create the list of repeat customers
    min = 0 + customerIdBase ; max = nHalf + customerIdBase
    repeatCustomers, randSeed = customerList(nHalf,countries,shape, scale, min, max, spark, randSeed)
    #print(repeatCustomers)

    #Create the list of onetime customers
    min = nHalf + customerIdBase ; max = n+ customerIdBase
    onetimeCustomers, randSeed = customerList(nHalf,countries,shape, scale, min, max, spark, randSeed)
    #print(onetimeCustomers)

    #Use the repeat and onetime customer lists to make a final list where the repeats each appear once in the first half
    #and have a chance of reappearing in the second half
    customerIds, customerNames, customerCountries, custCity = finalLists(nHalf, nHalf, repeatCustomers, onetimeCustomers, repeatProb, spark)
    #print(collections.Counter(customerCountries))

    #Create the lists of product information
    productIds, productNames, category, price, website = productList(n)
    #print(collections.Counter(category))

    #Create a dictionary of the data
    data2 = {"order_id":orderIds, "customer_id":customerIds, "customer_name":customerNames, "product_id":productIds, "product_name":productNames,"product_category":category, "price":price, "country":customerCountries, "city":custCity, "ecommerce_website_name":website}
    #print("order_id" + str(len(orderIds)) + " customer_id" + str(len(customerIds)) +  " customer_name" + str(len(customerNames)) +  " product_id" + str(len(productIds)) +  " product_name" + str(len(productNames)) + " product_category" + str(len(category)) + "price" + str(len(price)) + "country" + str(len(customerCountries)) +  "city" + str(len(custCity)) + "ecommerce_website_name" + str(len(website)))
    pdDf2 = pd.DataFrame(data2) #Pass the dictionary to pandas
    df2 = spark.createDataFrame(pdDf2) #pass the pandas dataframe to spark
    return df2
