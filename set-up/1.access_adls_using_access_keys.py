# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using access Keys
# MAGIC 1. Set the spark config fs.azure.account.keys
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formulacredentials = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1accountkey')

spark.conf.set("fs.azure.account.key.formula1555.dfs.core.windows.net", formulacredentials)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1555.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1555.dfs.core.windows.net/circuits.csv"))