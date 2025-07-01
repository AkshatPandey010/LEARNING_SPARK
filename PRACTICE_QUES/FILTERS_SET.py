# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import pandas as pd
df_xls = pd.read_excel("/FileStore/tables/Superstore_orders-1.xls", engine='xlrd')
df_spark = spark.createDataFrame(df_xls)

# COMMAND ----------

# DBTITLE 1,READING FILE DATA
orders_df = spark.read.format('csv').option('header','true').option('inferschema', 'true').load('/FileStore/tables/Superstore_orders.csv')
display(orders_df)

# COMMAND ----------

# 1- write a code to get all the orders where customers name has "a" as second character and "d" as fourth character (58 rows)

res1_df = orders_df.filter(
    (substring(col("Customer_Name"), 2, 1) == "a")
    & (substring(col("Customer_Name"), 4, 1) == "d")
)
display(res1_df)

# COMMAND ----------

# 2- write a code to get all the orders placed in the month of dec 2020 (352 rows)

res2_df = orders_df.filter(
    (col("Order_Date") >= "2020-12-01") & (col("Order_Date") <= "2020-12-31")
)
display(res2_df)

# COMMAND ----------

# 3- write a code to get all the orders where ship_mode is neither in 'Standard Class' nor in 'First Class' and ship_date is after nov 2020 (944 rows)

res3_df = orders_df.filter(
    (col("Ship_Mode") != "Standard Class") & (col("Ship_Mode") != "First Class")
    & (col("Ship_Date") > "2020-11-30")
)
display(res3_df)


# COMMAND ----------

# 4- write a code to get all the orders where customer name neither start with "A" and nor ends with "n" (6988 rows)

res4_df = orders_df.filter(
    (substring(col("Customer_Name"), 1, 1) != "A") & 
    (substring(reverse(col("Customer_Name")), 1, 1) != "n")
)
display(res4_df)
res4_df.count()


# COMMAND ----------

res = spark.sql("""
                select * from {orders} where Customer_Name not like 'A%' and Customer_Name not like '%n'
                """), orders = orders_df
display(res)
res.count()

# COMMAND ----------

# DBTITLE 1,JUST FOR SQL QUERIES
orders_df.createOrReplaceTempView("order_tbl")

# COMMAND ----------

# 5- write a code to get all the orders where profit is negative (1808 rows)

res5_df = orders_df.filter(col('Profit') < 0)
display(res5_df)

# COMMAND ----------

# 6- write a query to get all the orders where either quantity is less than 3 or profit is 0 (3389)

res6_df = orders_df.filter(
    (col("Quantity").cast("integer")  < 3) | (col("Profit") == 0)
)
display(res6_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from order_tbl where Region = 'South' and Discount > 0

# COMMAND ----------

# 7- your manager handles the sales for South region and he wants you to create a report of all the orders in his region where some discount is provided to the customers (815 rows)

res7_df = orders_df.filter(
    (col("Region") == "South") & (col("Discount").cast("integer") > 0)
)
display(res7_df)
