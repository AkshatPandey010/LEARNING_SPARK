# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)]

manager_df = spark.createDataFrame(data = emp_data, schema=['id','name','sal','mgr_id'])
display(manager_df)

# COMMAND ----------

#SELECTING UNIQUE RECORDS
display(manager_df.distinct())

# COMMAND ----------

#SELECTING SELECTED UNIQUE COLUMNS
display(manager_df.select('id','name').distinct())

# COMMAND ----------

#SORTING DATAFRAME
display(manager_df.sort(col('sal')))

#MULTIPLE COLUMN SORTING
display(manager_df.sort(col('sal').desc(), col('name').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC LEETCODE QUESTION

# COMMAND ----------

leet_code_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)]

question_df = spark.createDataFrame(data = leet_code_data, schema=['id', 'name', 'ref_id'])
display(question_df)

# COMMAND ----------

#QUESTION IS TO FIND NAME WHERE REF_ID != 2
display(question_df.select('name').filter((col('ref_id') != 2) | (col('ref_id').isNull())))