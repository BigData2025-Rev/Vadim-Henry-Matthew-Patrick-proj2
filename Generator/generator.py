from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from dateTimeGenerator import DateTimeGenerator
from payment_generate import PaymentGenerator
import customersProducts
import random

spark = SparkSession.builder.appName('S').getOrCreate()
sc = spark.sparkContext
schema = [
    'order_id',
    'customer_id',
    'customer_name',
    'product_id',
    'product_name',
    'product_category',
    'payment_type',
    'qty',
    'price',
    'dateTime',
    'country',
    'city',
    'ecommerce_website',
    'payment_txn_id',
    'payment_txn_success',
    'failure_reason'
]
dtg = DateTimeGenerator()
pyg = PaymentGenerator()
d = customersProducts.makeDict(spark)
def get_generated_row(i):
    order_id = d['order_id'][i]
    cust_id = d['customer_id'][i]
    cust_name = d['customer_name'][i]
    prod_id = d['product_id'][i]
    prod_name = d['product_name'][i]
    prod_cat = d['product_category'][i]
    qty = random.choices(range(11),[0,80,15,3,2,1,.5,.4,.4,.4,.4])[0]
    price = d['price'][i]
    country = d['country'][i]
    city = d['city'][i]
    web_name = d['ecommerce_website_name'][i]
    payment = pyg.generate_payments(1)[0]
    p_type = payment["payment_type"]
    p_id = payment["payment_txn_id"]
    pts = payment['payment_txn_success']
    fail = payment['failure_reason']
    date = dtg.gen_date()
    return (order_id, cust_id, cust_name, prod_id, prod_name, prod_cat, p_type, qty, price, date, country, city, web_name, p_id, pts, fail)
rows = []
for i in range(15000):
    rows.append(get_generated_row(i))
df = pd.DataFrame(rows,columns=schema)
df.to_csv("generatedData.csv",encoding='utf-8', index=False)
