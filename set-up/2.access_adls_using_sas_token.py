# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formulacredentials = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-SASToken')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1555.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1555.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1555.dfs.core.windows.net", formulacredentials)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1555.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1555.dfs.core.windows.net/circuits.csv"))