# Databricks notebook source
# MAGIC %md
# MAGIC ###Partitions

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

# COMMAND ----------

#jdbc config
jdbc_url = "jdbc:sqlserver://server-training.database.windows.net:1433;database=DB_training"
jdbc_prop = {
    "user" : "sqladmin",
    "password" : "admin301428<3",
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
table_name = "SalesLT.Customer"

# COMMAND ----------

df = spark.read.jdbc(url = jdbc_url,table = table_name, properties = jdbc_prop)

# COMMAND ----------

df.display()

# COMMAND ----------

#Get number of partitions
df.rdd.getNumPartitions()

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

df = df.coalesce(4)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cache and Persist

# COMMAND ----------

df.cache().count()

# COMMAND ----------

df.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------


