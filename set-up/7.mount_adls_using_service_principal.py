# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, Tenant_Id and client_Secrets from Key Vault
# MAGIC 2. Set the spark config with App/ClientID, Directory/TenantID & Secret
# MAGIC 4. Call file system utlity mount to mount the storage
# MAGIC 3. Explore other file system utlities related to mount (list all mounts, unmnount)
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-ServicePrincipal-clientid')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-seviceprincipal-tenantid')
client_Secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-serviceprincipal-clientsecret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_Secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1555.dfs.core.windows.net",
  mount_point = "/mnt/formula1555/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1555/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1555/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# Unmount
dbutils.fs.unmount("/mnt/formula1555/demo")