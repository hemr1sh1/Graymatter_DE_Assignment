-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Cloning

-- COMMAND ----------

select * from personemployer

-- COMMAND ----------

create table delta.`abfss://adb-container@saadlsg21717.dfs.core.windows.net/ext_final_DClone` clone delta.`abfss://adb-container@saadlsg21717.dfs.core.windows.net/ext_final`

-- COMMAND ----------

--Creating Shallow Clone
create table personemployer_SClone shallow clone personemployer

-- COMMAND ----------

--Creating Deep Clone
create table personemployer_DClone clone personemployer

-- COMMAND ----------

select * from personemployer

-- COMMAND ----------

select * from personemployer_DClone

-- COMMAND ----------

select * from personemployer_SClone

-- COMMAND ----------

insert into personemployer values (9,"Mithun","Bengaluru","QualComm")

-- COMMAND ----------

insert into personemployer_DClone values (10,"Vedasri","London","Amadeus")

-- COMMAND ----------

insert into personemployer_SClone values (11,"Thilak","Bengaluru","Cisco")

-- COMMAND ----------

describe history personemployer_DClone

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Inference - Both clones and source table exist as independent tables...because they maintain their own metadata

-- COMMAND ----------

-- Trying out create or replace with version
CREATE OR REPLACE TABLE personemployer_DClone  CLONE personemployer version as of 0

-- COMMAND ----------


