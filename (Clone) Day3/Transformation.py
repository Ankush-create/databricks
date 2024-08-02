# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/

# COMMAND ----------

EL (Transformation) SQL

E T(Pyspark or SQL) L

# COMMAND ----------

Data Objects in the lakehouse
Catalog
schema/Databases
Table, view, functions

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/circuits.csv",header=True, inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

Transformation using pyspark
1. DataFrame Functions (https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- .select()
- .alias()
- .withColumnRenamed
- .withColumn


2. Functions (https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- col
- concat
- lit

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id")).display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.select(col("circuitId"),"name",df["location"]).display()

# COMMAND ----------

display(df)

# COMMAND ----------

df.select(concat("location"," ", "country")).display()

# COMMAND ----------

df.select(concat("location",lit(" "), "country")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

df\
.withColumn("ingestion_date",current_date())\
.withColumn("path",input_file_name())\
.drop("url")\
.display()

# COMMAND ----------

df.withColumn("circuitId",current_date()).display()
