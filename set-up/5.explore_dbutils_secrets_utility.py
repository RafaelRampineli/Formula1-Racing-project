# Databricks notebook source
# MAGIC %md
# MAGIC # Expore the capabilities of the dbutils.secrets utility
# MAGIC
# MAGIC Before create a secret scope, must create an Key Vault and a Secrets on Azure.
# MAGIC
# MAGIC To create a secret scope access: MENU principal and add on URL : #secrets/createScope
# MAGIC
# MAGIC DNS Name is the Vault URI from Key Vault (properties)
# MAGIC ResourceID is the Resource ID 

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------

formulacredentials = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1accountkey')

spark.conf.set("fs.azure.account.key.formula1555.dfs.core.windows.net", formulacredentials)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1555.dfs.core.windows.net"))               

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1555.dfs.core.windows.net/circuits.csv"))