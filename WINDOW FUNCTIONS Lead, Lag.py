# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]
prod_df = spark.createDataFrame(data = product_data , schema = ['cmp_id', 'name', 'date', 'sales'])
display(prod_df)

# COMMAND ----------

window = Window.partitionBy('cmp_id').orderBy('date')
#LAG FUNCTION
last_month_df = prod_df.withColumn('last_mon_sale', lag(col('sales'),1).over(window))
display(last_month_df)

# COMMAND ----------

#LEAD FUNCTION
next_month_df = prod_df.withColumn('next_mon_sale', lead(col('sales'), 1).over(window))
display(next_month_df)

# COMMAND ----------

#%AGE OF LOSS OR GAIN BASED ON PREVIOUS MONTH SALES
res_df = last_month_df.withColumn(
    "prof_or_loss",
    round(((col("sales") - col("last_mon_sale")) / col("sales")) * 100, 2),
)
display(res_df)

# COMMAND ----------

# WHAT IS THE %AGE OF SALES EACH MONTH BASED ON LAST 6 MONTH SALES
ans_df = prod_df.withColumn(
    "tot_sale", sum("sales").over(Window.partitionBy("cmp_id"))
).withColumn("%sale_per_month", round((col("sales") / col("tot_sale")) * 100, 2))
display(ans_df)