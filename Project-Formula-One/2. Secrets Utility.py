# Databricks notebook source
print('hello')

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

display(dbutils.secrets.list("Dev-scope"))

# COMMAND ----------

key = dbutils.secrets.get(scope = "Dev-scope", key = "Account-key")

# COMMAND ----------

display(key)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.sadevadls1.dfs.core.windows.net", key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@sadevadls1.dfs.core.windows.net"))
