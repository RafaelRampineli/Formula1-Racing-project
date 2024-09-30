# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set the spark config with App/ClientID, Directory/TenantID & Secret
# MAGIC 4. Assign Role "Storage Blob Data Contributor" to the Data Lake
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-ServicePrincipal-clientid')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-seviceprincipal-tenantid')
client_Secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-serviceprincipal-clientsecret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1555.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1555.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1555.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1555.dfs.core.windows.net", client_Secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1555.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1555.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1555.dfs.core.windows.net/circuits.csv"))