# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

sale_df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
display(sale_df)

# COMMAND ----------

from pyspark.sql.window import *
r_df = sale_df.withColumn('row_num', row_number().over(Window.orderBy(desc('Item_Outlet_Sales'))))
display(r_df)

# COMMAND ----------

# ANOTHER EXAMPLE
emp_data = [
    (1, "manish", 50000, "IT", "m"),
    (2, "vikash", 60000, "sales", "m"),
    (3, "raushan", 70000, "marketing", "m"),
    (4, "mukesh", 80000, "IT", "m"),
    (5, "priti", 90000, "sales", "f"),
    (6, "nikita", 45000, "marketing", "f"),
    (7, "ragini", 55000, "marketing", "f"),
    (8, "rashi", 100000, "IT", "f"),
    (9, "aditya", 65000, "IT", "m"),
    (10, "rahul", 50000, "marketing", "m"),
    (11, "rakhi", 50000, "IT", "f"),
    (12, "akhilesh", 90000, "sales", "m"),
]
emp_df = spark.createDataFrame(data= emp_data, schema= ['id', 'name', 'salary', 'dep', 'sex'])
display(emp_df)

# COMMAND ----------

# FIRST WAY IS TO DEFINE WINDOW SEPARATELY
window = Window.partitionBy("dep").orderBy("salary")
res = emp_df.withColumn("row_num", row_number().over(window))
display(res)

# COMMAND ----------

#SECOND WAY IS TO INCLUDE WINDOW IN OVER CLAUSE ONLY {don't know why but my prefered way}
res1 = emp_df.withColumn("row_num", row_number().over(Window.partitionBy('dep').orderBy('salary')))
display(res1)

# COMMAND ----------

# MULTIPLE COLUMNS USING WINDOW FUNCTIONS
# FOR THIS DEFINING A WINDOW EARLIER IS PREFERED FOR CLEANER CODE MORE REUSABLE CODE
window = Window.partitionBy("dep").orderBy("salary")
window2 = Window.partitionBy('dep').orderBy(desc('salary'))
res2 = (
    emp_df.withColumn('rev_row_num', row_number().over(window2))
    .withColumn("row_num", row_number().over(window))
    .withColumn("rnk", rank().over(window))
    .withColumn("d_rnk", dense_rank().over(window))

)
display(res2)