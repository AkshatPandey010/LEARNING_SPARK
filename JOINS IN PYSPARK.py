# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

customer_data = [
    (1, "manish", "patna", "30-05-2022"),
    (2, "vikash", "kolkata", "12-03-2023"),
    (3, "nikita", "delhi", "25-06-2023"),
    (4, "rahul", "ranchi", "24-03-2023"),
    (5, "mahesh", "jaipur", "22-03-2023"),
    (6, "prantosh", "kolkata", "18-10-2022"),
    (7, "raman", "patna", "30-12-2022"),
    (8, "prakash", "ranchi", "24-02-2023"),
    (9, "ragini", "kolkata", "03-03-2023"),
    (10, "raushan", "jaipur", "05-02-2023"),
]
customer_schema = ["customer_id", "customer_name", "address", "date_of_joining"]
sales_data = [
    (1, 22, 10, "01-06-2022"),
    (1, 27, 5, "03-02-2023"),
    (2, 5, 3, "01-06-2023"),
    (5, 22, 1, "22-03-2023"),
    (7, 22, 4, "03-02-2023"),
    (9, 5, 6, "03-03-2023"),
    (2, 1, 12, "15-06-2023"),
    (1, 56, 2, "25-06-2023"),
    (5, 12, 5, "15-04-2023"),
    (11, 12, 76, "12-03-2023"),
]
sales_schema = ["customer_id", "product_id", "quantity", "date_of_purchase"]
product_data = [
    (1, "fanta", 20),
    (2, "dew", 22),
    (5, "sprite", 40),
    (7, "redbull", 100),
    (12, "mazza", 45),
    (22, "coke", 27),
    (25, "limca", 21),
    (27, "pepsi", 14),
    (56, "sting", 10),
]
product_schema = ["id", "name", "price"]

# COMMAND ----------

customer_df = spark.createDataFrame(data=customer_data, schema=customer_schema)
sales_df = spark.createDataFrame(data=sales_data, schema=sales_schema)
product_df = spark.createDataFrame(data=product_data, schema=product_schema)

# COMMAND ----------

display(customer_df)
display(sales_df)
display(product_df)

# COMMAND ----------

#RENAMING COLUMN FOR BETTER UNDERSTANDING
customer_df = customer_df.withColumnRenamed('customer_id', 'cust_id')

# COMMAND ----------

joined_df = customer_df.join(
    sales_df, sales_df["customer_id"] == customer_df["cust_id"], "inner"
)
display(
    joined_df.select(
        "cust_id",
        "customer_name",
        col("customer_id").alias("sales_tbl_cust_id"),
        "product_id",
        "quantity",
    )
)

# COMMAND ----------

# COUNTING RECORDS PER CUSTOMER
temp_df = joined_df.select(
    "cust_id",
    "customer_name",
    col("customer_id").alias("sales_tbl_cust_id"),
    "product_id",
    "quantity",
)
display(temp_df.groupBy("cust_id").agg(count("product_id").alias("count")))

# COMMAND ----------

#MULTIPLE COLUMN JOIN
customer_df.join(sales_df, (sales_df['customer_id'] == customer_df['cust_id']) & (customer_df['another_cloumn'] == sales_df['another_column']), 'inner')



# COMMAND ----------

# JOINING MULTIPLE TABLES
multiple_join_df = customer_df.join(
    sales_df, sales_df["customer_id"] == customer_df["cust_id"], "inner"
).join(product_df, sales_df["product_id"] == product_df["id"], "left")
res_df = multiple_join_df.select("cust_id", "product_id", "quantity", "name", "price")
display(res_df)

# COMMAND ----------

# CALCULATE TOTAL PURCHASE AMOUNT PER CUSTOMER AND PER PRODUCT
calc_df = res_df.groupBy("cust_id", "product_id").agg(
    sum("price").alias("tot_purchase_amt")
)
display(calc_df)

# COMMAND ----------

