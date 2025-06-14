# Databricks notebook source
display(spark.read.format("json").option("inferSchema", "true").option("mode","PERMISSIVE").load("/FileStore/tables/line_delimited_json.json"))

# COMMAND ----------

display(spark.read.format("json").option("inferSchema", "true").option("mode","PERMISSIVE").load("/FileStore/tables/single_file_json_with_extra_fields.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC Multiline json should be passsed as list of multiple dicts(key value pairs)

# COMMAND ----------

display(spark.read.format("json").option("inferSchema", "true").option("mode","PERMISSIVE").option("multiline", "true").load("/FileStore/tables/Multi_line_correct.json"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading corrupted json data

# COMMAND ----------

display(spark.read.format("json").option("inferSchema", "true").option("mode","PERMISSIVE").load("/FileStore/tables/corrupted_json.json"))