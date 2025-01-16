import customersProducts
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
import pandas as pd
import numpy as np
from pyspark.sql import functions as sf
spark = SparkSession.builder.getOrCreate()

dataframe = customersProducts.makeDataFrame(spark)
dataframe.show()

spark.stop()