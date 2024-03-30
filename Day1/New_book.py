# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.drop("url")

# COMMAND ----------

df2=df1.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df2.write.saveAsTable("deepak.consructor")

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json `dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json. `dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------


