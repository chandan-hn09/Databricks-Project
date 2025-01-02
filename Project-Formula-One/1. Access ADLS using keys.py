# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Access ADLS using Account key

# COMMAND ----------

print('hi')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.sadevadls1.dfs.core.windows.net", "")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@sadevadls1.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Access ADLS using SAS

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sadevadls1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sadevadls1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.sadevadls1.dfs.core.windows.net", "")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@sadevadls1.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Access ADLS using SP

# COMMAND ----------

client_id=''
tenant_id=''
client_secret=''

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sadevadls1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sadevadls1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sadevadls1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.sadevadls1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sadevadls1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(spark.read.csv("abfss://demo@sadevadls1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


