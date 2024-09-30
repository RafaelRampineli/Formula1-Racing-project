# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Countainers for the project

# COMMAND ----------

def mount_adls (storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-ServicePrincipal-clientid')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-seviceprincipal-tenantid')
    client_Secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-serviceprincipal-clientsecret')

    # Set Spark Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_Secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # If mounted, display and return
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        display(dbutils.fs.mounts())
        return
            
    # Mount the storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Containers

# COMMAND ----------

mount_adls('formula1555','demo')

# COMMAND ----------

mount_adls('formula1555','raw')

# COMMAND ----------

mount_adls('formula1555','processed')

# COMMAND ----------

mount_adls('formula1555','presentation')