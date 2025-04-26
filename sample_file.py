# Databricks notebook source
print('Hi')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

spark = SparkSession.builder.appName('databricks-learning').getOrCreate()

# COMMAND ----------

df = spark.range(1, 15000000000, 1, 10)


# COMMAND ----------

new_df = df.withColumn('temp', F.rand())

# COMMAND ----------

new_df.write.mode('overwrite').saveAsTable('temp')

# COMMAND ----------

spark.sql('create catalog if not exists databricks_learning')

# COMMAND ----------

spark.sql('create database databricks_learning.dummy')

# COMMAND ----------

spark.sql('use databricks_learning.dummy')

# COMMAND ----------


