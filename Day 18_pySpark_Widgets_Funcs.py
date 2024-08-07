# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run utility notebook

# COMMAND ----------

# MAGIC %run /Workspace/Users/azuretraininggm@gmail.com/Utilities/Utility_Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ###Widgets

# COMMAND ----------

df_widget = spark.read.option("header",True).option("inferschema",True).csv("/FileStore/ADB_Training/ADB_Age.csv")

# COMMAND ----------

df_widget.display()

# COMMAND ----------

df_1 = df_widget.filter(df_widget.Name==w_t_name)#w_t_name from utility table
df_1.display()

# COMMAND ----------

df_2 = df_widget.filter(df_widget.Age<w_cb_age)#w_cb_age from utility table
df_2.display()

# COMMAND ----------

df_widget.createOrReplaceTempView("Temp")
df_3 = spark.sql(f"""select * from Temp where ID = {w_dd_id}""")#w_dd_id from utility table
df_3.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Temp where ID in ($MS_ID)--MS_ID from utility table

# COMMAND ----------

# MAGIC %md
# MAGIC ###User Defined Functions

# COMMAND ----------

df_udf = spark.read.format("csv").option("header",True).option("inferschema",True).load("/mnt/adb-container/Transaction.csv")

# COMMAND ----------

df_udf.display()

# COMMAND ----------

#Using func in pySpark
df_udf_dol = df_udf.withColumn("AmountDol",pSF_RsToDol(col("Amount")))#pSF_RsToDol from utility table

# COMMAND ----------

df_udf_dol.display()

# COMMAND ----------

df_udf.createOrReplaceTempView("TempUDF")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Using func in SQL
# MAGIC select *,sqlF_RsToDol(Amount) as AmountDol from TempUDF --sqlF_RsToDol from utility table

# COMMAND ----------

# MAGIC %md
# MAGIC ###Triggering a notebook from a different notebook

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/azuretraininggm@gmail.com/Utilities/Utility_Notebook",20)

# COMMAND ----------


