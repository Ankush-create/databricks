# Databricks notebook source


# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/

# COMMAND ----------

Task:
    
Step1: Read the csv

Step 2: Transformation 
- rename raceId to race_id and circuitId to circuit_id
- new column that should contain current timestamp
- new column that should contain path 
- drop url column

step3: save it to table

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/races.csv",header=True,inferSchema=True)

# COMMAND ----------

df1= df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("path",input_file_name())\
.drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.saveAsTable("datamaster.formula1.races")
