# Databricks notebook source
#Import types and functions
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Create schema
sch = StructType([
  StructField("ID",IntegerType(),True),
  StructField("Name",StringType(),True),
  StructField("Age",IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Different Read Modes

# COMMAND ----------

#Dont load table at alll even if one value is bad
df_rm1 = spark.read.option("header",True).schema(sch).option("mode","failfast").csv("/FileStore/ADB_Training/ADB_Age.csv")

# COMMAND ----------

df_rm2 = spark.read.option("header",True).schema(sch).format("csv").load("/FileStore/ADB_Training/ADB_Age.csv")

# COMMAND ----------

#Drop rows with bad values like null
df_rm3 = spark.read.option("header",True).schema(sch).option("mode","Dropmalformed").csv("/FileStore/ADB_Training/ADB_Age.csv")

# COMMAND ----------

df_rm1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mounting

# COMMAND ----------

dbutils.fs.mount(source = "wasbs://adb-container@saadlsg21717.blob.core.windows.net",mount_point ="/mnt/adb-container",extra_configs = {"fs.azure.account.key.saadlsg21717.blob.core.windows.net":dbutils.secrets.get(scope = "SecretADB", key = "SecretStorage")})

# COMMAND ----------

dbutils.fs.ls("/mnt")

# COMMAND ----------

df_wm1 = spark.read.option("header",True).option("inferschema",True).csv("/mnt/adb-container/Person.csv")
df_wm2 = spark.read.format("csv").option("header",True).option("inferschema",True).load("/mnt/adb-container/Employer.csv")

# COMMAND ----------

df_wm = df_wm1.join(df_wm2,df_wm1.PID==df_wm2.PID,how="inner").select(df_wm1['*'],df_wm2["PEmployer"])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Modes

# COMMAND ----------

#Save as csv
df_wm.write.csv("/mnt/adb-container/output")

# COMMAND ----------

#Save as a table
df_wm.write.format("delta").saveAsTable("PersonEmployer")

# COMMAND ----------

#Save as parquet
df_wm.write.format("parquet").mode("append").save("/mnt/adb-container/output")

# COMMAND ----------

# MAGIC %md
# MAGIC ###creating an external table and saving it on DB after that

# COMMAND ----------

df_wm.write.format("delta").save("/mnt/adb-container/external")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Time travelling feature of Delta logs

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ext_Emp_Sal
# MAGIC using csv
# MAGIC OPTIONS (path "abfss://adb-container@saadlsg21717.dfs.core.windows.net/emp_sal.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ext_personemployer

# COMMAND ----------

_sqldf.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ext_personemployer values (9,"Thilak","Bengaluru","Cisco")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from ext_personemployer where PID = 3
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update ext_personemployer
# MAGIC set PName = "Yogitha" where PID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC --To get previous versions
# MAGIC describe history ext_personemployer

# COMMAND ----------

# MAGIC %sql
# MAGIC --Restoring to orignal state
# MAGIC restore table ext_personemployer to version as of 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ext_personemployer

# COMMAND ----------

# MAGIC %md
# MAGIC ###Optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC --Spamming inserts
# MAGIC insert into ext_personemployer values (9,"Thilak","Bengaluru","Cisco"),(10,"Vedasri","London","Amadeus"),(11,"Mithun","Bengaluru","Qualcomm")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ext_personemployer values (13,"Jorge","Montreal","MadeByGather")

# COMMAND ----------

# MAGIC %sql
# MAGIC --zorder and compaction optimized
# MAGIC optimize ext_personemployer
# MAGIC zorder by (PCity)

# COMMAND ----------

# MAGIC %sql
# MAGIC --CLusterBy
# MAGIC alter table ext_personemployer
# MAGIC cluster by (PID,PName)

# COMMAND ----------


