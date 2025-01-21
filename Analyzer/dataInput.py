from pyspark.sql import SparkSession
import re
import pandas as pd

spark = SparkSession.builder.appName('S').getOrCreate()
sc = spark.sparkContext
# schema = [
#     'order_id',
#     'customer_id',
#     'customer_name',
#     'product_id',
#     'product_name',
#     'product_category',
#     'payment_type',
#     'qty',
#     'price',
#     'dateTime',
#     'country',
#     'city',
#     'ecommerce_website',
#     'payment_txn_id',
#     'payment_txn_success',
#     'failure_reason'
# ]

def main():
    file = 'P2output.csv'
    df = readData(file)
    df.show()
    df = cleanData(df)
    df.show()
    #df = cleanData(df)
    pdDf = df.toPandas()
    pdDf.to_csv("cleanedData.csv",encoding='utf-8', index=False)
    

def readData(file):
    df = spark.read.option("header", True).csv(file)
    return df

def cleanData(df):
    df = df.na.drop() #Remove rows with null values

    #Filter out rows with invalid characters. [^a-zA-Z -] gets rows with invalid characters
    #~ is a not function to exclude those rows
    df = df.filter((~df.customer_name.rlike("[^a-zA-Z' -]")))
    df = df.filter((~df.product_name.rlike("[^a-zA-Z' -]")))
    df = df.filter((~df.product_category.rlike("[^a-zA-Z' -]")))
    df = df.filter((~df.city.rlike("[^a-zA-Z' -]")))
    return df

main()