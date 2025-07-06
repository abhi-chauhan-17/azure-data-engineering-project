# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA ACCESS USING APP

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.abhiawstoragedl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.abhiawstoragedl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.abhiawstoragedl.dfs.core.windows.net", "d1d0f81a-a1aa-4700-aa6c-c3ccb4cbf468")
spark.conf.set("fs.azure.account.oauth2.client.secret.abhiawstoragedl.dfs.core.windows.net", "u0I8Q~bW9oDmpSv5Lcd~zpCQsor98w5gZVaRYaV8")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.abhiawstoragedl.dfs.core.windows.net", "https://login.microsoftonline.com/d9dde6cb-8304-4683-b7ac-94af39828749/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Data

# COMMAND ----------

df_cal = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cus = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_procat = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_pro = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sal = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_subcat = spark.read.format("csv").option("header", True).option("inferSchema",True).load("abfss://bronze@abhiawstoragedl.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calendar

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month',month(col('Date')))\
            .withColumn('Year',year(col('Date')))\
            .withColumn('Day',day(col('Date')))
df_cal.display()


# COMMAND ----------

df_cal.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Calendar").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Customer

# COMMAND ----------

df_cus.display()

# COMMAND ----------

# df_cus = df_cus.withColumn("FullName",concat(col("Prefix"),lit(' '),col("FirstName"),lit(' '),col("LastName")))
df_cus = df_cus.withColumn("FullName",concat_ws(' ',col("Prefix"),col("FirstName"),col("LastName")))
df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Customers").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####SubCategory

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_SubCategories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Products").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Returns").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Territories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sales

# COMMAND ----------

df_sal.display()

# COMMAND ----------

df_sal = df_sal.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sal = df_sal.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sal = df_sal.withColumn('multiply',col('OrderQuantity')*col('OrderLineItem'))

# COMMAND ----------

df_sal.display()

# COMMAND ----------

df_sal.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@abhiawstoragedl.dfs.core.windows.net/AdventureWorks_Sales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sales Analysis

# COMMAND ----------

df_sal.groupBy('OrderDate').agg(count('OrderNumber').alias('TotalOrder')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

