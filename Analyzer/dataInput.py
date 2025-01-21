from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('S').getOrCreate()
sc = spark.sparkContext

def readData(file):
    df = spark.read.option("header", True).csv(file)
    df.show()

file = 'P2output.csv'
readData(file)