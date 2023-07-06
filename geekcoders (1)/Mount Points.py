# Databricks notebook source
# MAGIC %scala
# MAGIC val containerName = dbutils.secrets.get(scope="geekcoders-secret",key="containername")
# MAGIC val storageAccountName = dbutils.secrets.get(scope="geekcoders-secret",key="storageaccountname")
# MAGIC val sas = dbutils.secrets.get(scope="geekcoders-secret",key="sas2")
# MAGIC val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC   source = dbutils.secrets.get(scope="geekcoders-secret",key="blob-mnt-path-2"),
# MAGIC   mountPoint = "/mnt/source_blob_new/",
# MAGIC   extraConfigs = Map(config -> sas))

# COMMAND ----------

# MAGIC %py
# MAGIC dbutils.fs.ls('/mnt/source_blob_new/')

# COMMAND ----------

# MAGIC %py
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "geekcoders-secret", key = "data-app-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="geekcoders-secret",key="data-app-secret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope = "geekcoders-secret", key = "data-client-refresh-url-2")}
# MAGIC
# MAGIC #Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC mountPoint="/mnt/raw_datalake/"
# MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = dbutils.secrets.get(scope = "geekcoders-secret", key = "raw-datalake"),
# MAGIC     mount_point = mountPoint,
# MAGIC     extra_configs = configs)
# MAGIC

# COMMAND ----------

# Listing all the files
dbutils.fs.ls('/mnt/raw_datalake/')

# COMMAND ----------

# MAGIC %py
# MAGIC # Getting SPN from Keyvaults
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "geekcoders-secret", key = "data-app-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="geekcoders-secret",key="data-app-secret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope = "geekcoders-secret", key = "data-client-refresh-url-2")}
# MAGIC
# MAGIC #Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC mountPoint="/mnt/cleansed_datalake/"
# MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
# MAGIC   dbutils.fs.mount(
# MAGIC     source = dbutils.secrets.get(scope = "geekcoders-secret", key = "datalake-cleansed"),
# MAGIC     mount_point = mountPoint,
# MAGIC     extra_configs = configs) 

# COMMAND ----------

dbutils.fs.ls('/mnt/cleansed_datalake')

# COMMAND ----------



# COMMAND ----------


