# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]
emp_schema = ['id','name','age','salary','country','dept']
emp_df = spark.createDataFrame(data = emp_data, schema = emp_schema)

# COMMAND ----------

# CONDITION IS TO CHECK ADULTS IN THIS DATA 
# HERE WE CAN USE when(condition1, 'outputvalue1').when(condition2, 'outputvalue2').otherwise('outputvalue3')
display(emp_df.withColumn("is_adult", when(col('age')<18, 'No').when(col("age")>18, "Yes").otherwise("NoValue")))

# COMMAND ----------

#NOW THE SCENARIO IS THAT WE HAVE TO FIRST HANDLE THE NULL AGES AND THEN CREATE IS_ADULT COLUMN
display(emp_df.withColumn("age", when(col('age').isNull(), lit(19)).otherwise(col('age')))\
               .withColumn("is_adult", when(col('age')>18, 'Yes').otherwise('No')))

# COMMAND ----------

#NOW SCENARIO IS TO CREATE A COLUMN FOR AGE GROUP OF PEOPLE 

display(emp_df.withColumn("age", when(col('age').isNull(), lit(19)).otherwise(col('age')))\
      .withColumn("age_group", when((col('age')>0) & (col('age')<18), "Minor").when((col('age')>= 18) & (col('age')<30), "Mid").otherwise("Buddhha")))