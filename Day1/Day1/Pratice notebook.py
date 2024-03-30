# Databricks notebook source
df1.write.saveAsTable("naval.circuit")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

df=spark.write.parquet("dbfs:/FileStore/tables/output/deepak/circuit")

# COMMAND ----------

df.write.parquet("dbfs:/FileStore/tables/output/deepak/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists deepak

# COMMAND ----------

df.write.saveAsTable('deepak.circuit')

# COMMAND ----------


