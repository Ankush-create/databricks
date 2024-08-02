# Databricks notebook source
https://datamasterconsultingin-my.sharepoint.com/:f:/g/personal/naval_thedatamaster_in/ElPi0OD3gn1CsE6zMMGcYzkB-mg_vKJK9AmnuiE2Cu4QIw?e=x1WJOk

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Extract the data/ Reading
# MAGIC
# MAGIC -- PySpark
# MAGIC -- Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL 
# MAGIC -- Querying the files(JSON, parquet, delta)
# MAGIC -- select * from file_format.`path`  (back tick, below esc key)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog datamaster;
# MAGIC create schema formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table formula1.constructor as 
# MAGIC select * from json.`dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select constructorId, nationality, name from formula1.constructor where nationality= 'British'

# COMMAND ----------


