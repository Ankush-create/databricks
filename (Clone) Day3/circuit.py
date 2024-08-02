# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/circuits.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table circuit (circuitid int, )

# COMMAND ----------

Spark: Lazy Evluation
Dataset:
    1.RDD 
    2.DataFrame
    3.Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extracting the data

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

Extract/Read

(Cloud Storage, ADLS/Blob, S3, GCS, Databases: MYSQL, SSMS, PostgreSQL, DW: Snowflake, Redshift, synpase, API:, kafka )
(Format: CSV, json, parquet, orc, delta, avro, excel,text, binary,html,xml)


Transformation 
(pyspark/sql)

Load/Write/saving: FILE or TABLE
FORMAT: (Format: CSV, json, parquet, orc, delta, avro, excel,text, binary,html,xml)
where: datalake, DW, lakehouse


(Recommended)
Load/Write/saving: FILE or TABLE
FORMAT: DELTA 
where: lakehouse


# COMMAND ----------

df.write.saveAsTable("datamaster.formula1.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamaster.formula1.circuit
