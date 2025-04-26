# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

spark = SparkSession.builder.appName('databricks-learning').getOrCreate()

# COMMAND ----------

spark.sql('create catalog if not exists databricks_learning')
spark.sql('create database if not exists databricks_learning.dummy')
spark.sql('use databricks_learning.dummy')

# COMMAND ----------

df = spark.range(1, 15000, 1, 10)
new_df = df.withColumn('temp', F.rand())
new_df.write.mode('overwrite').saveAsTable('temp')


# COMMAND ----------


