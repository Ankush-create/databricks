# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/pit_stops.json`

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/pit_stops.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/pit_stops.json")

# COMMAND ----------

df.display()

# COMMAND ----------


