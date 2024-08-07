# Databricks notebook source
# MAGIC %md
# MAGIC ###Utility Notebook

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#creating widgets
dbutils.widgets.text("T_Name","John")
dbutils.widgets.dropdown("DD_ID","1",["1","2","3","4"])
dbutils.widgets.combobox("CB_Age","30",["30","33"])
dbutils.widgets.multiselect("MS_ID","1",["1","2","3","4"])

# COMMAND ----------

#Assigning widgets to variables
w_t_name = dbutils.widgets.get("T_Name")
w_dd_id = dbutils.widgets.get("DD_ID")
w_cb_age  =dbutils.widgets.get("CB_Age")
w_ms_id = dbutils.widgets.get("MS_ID")

# COMMAND ----------

#udf
def RsToDol(AmountRs):
    return AmountRs*83

# COMMAND ----------

#Registering function with sql and pySpark
pSF_RsToDol = udf(RsToDol,IntegerType())
spark.udf.register("sqlF_RsToDol",RsToDol,IntegerType())

# COMMAND ----------


