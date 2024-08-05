# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_01 = spark.read.option("header",True).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df_01.show()

# COMMAND ----------

df_01.display()

# COMMAND ----------

df_01.printSchema()

# COMMAND ----------

df_02 = spark.read.option("header",True).option("inferschema",True).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df_02.printSchema()

# COMMAND ----------

sch = StructType().add("App",StringType(),True)\
    .add("Category",StringType(),True)\
    .add("Rating",DoubleType(),True)\
    .add("Reviews",IntegerType(),True)\
    .add("Size",StringType(),True)\
    .add("Installs",StringType(),True)\
    .add("Type",StringType(),True)\
    .add("Price",StringType(),True)\
    .add("Content Rating",StringType(),True)\
    .add("Genres",StringType(),True)\
    .add("Last Updated",StringType(),True)\
    .add("Current Ver",StringType(),True)\
    .add("Android Ver",StringType(),True)

# COMMAND ----------

df_03 = spark.read.option("header",True).schema(sch).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df_03.printSchema()

# COMMAND ----------

df_03.display()

# COMMAND ----------

df_03 = df_03.withColumn("R*R",col("Rating")*col("Reviews"))

# COMMAND ----------

df_03 = df_03.withColumn("AppGC",concat(col("Category"),col("Genres")))

# COMMAND ----------

df_03 = df_03.withColumn("Owner",lit("SDP"))

# COMMAND ----------

df_03 = df_03.withColumn("AppAgain",col("App"))

# COMMAND ----------

df_03.printSchema()

# COMMAND ----------

df_03.display()

# COMMAND ----------

df_chain = df_03.withColumn("R*R",col("Rating")*col("Reviews")).withColumn("AppGC",concat(col("Category"),col("Genres"))).withColumn("Owner",lit("SDP"))

# COMMAND ----------

df_chain.display()

# COMMAND ----------

df_03 = df_03.withColumnRenamed("App","AppName")

# COMMAND ----------

df_s = df_03.select("AppName")

# COMMAND ----------

df_sE = df_03.selectExpr('cast(Rating as integer) as New_Rating')

# COMMAND ----------

df_sE.display()

# COMMAND ----------

df_03.count()

# COMMAND ----------

df_03.distinct().count()

# COMMAND ----------

df_dD = df_03.dropDuplicates(["Category","Reviews"])

# COMMAND ----------

df_dD.count()

# COMMAND ----------

df_if = df_03.withColumn("RatingW",when((col("Rating")>3) & (col("Rating")<=4), "Average").when((col("Rating")>4) & (col("Rating")<=5), "Good").otherwise(lit("Bad")))

# COMMAND ----------

df_if.display()

# COMMAND ----------

sch1 = StructType().add("App",StringType(),True).add("Translated_Review",StringType(),True).add("Sentiment",StringType(),True).add("Sentiment_Polarity",DoubleType(),True).add("Sentiment_Subjectivity",DoubleType(),True)

# COMMAND ----------

df_user = spark.read.option("header",True).schema(sch1).csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df_join_sent = df_03.join(df_user,df_03.AppName==df_user.App,how="inner").select(df_03['*'],df_user["Sentiment"])

# COMMAND ----------

df_join_sent.display()

# COMMAND ----------

df_joined_avgrating = df_join_sent.groupBy("AppName").agg(avg("Rating").alias("AvgRating"))

# COMMAND ----------

df_joined_avgrating.sort("AppName").display()

# COMMAND ----------


