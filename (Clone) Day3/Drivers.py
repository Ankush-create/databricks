# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/drivers.json")

# COMMAND ----------

df.write.saveAsTable("datamaster.formula1.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table datamaster.formula1.drivers2 as 
# MAGIC select * from json.`dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/drivers.json`
