# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@datamasterconsultingin.onmicrosoft.com/Hexware Training/Day3/includes"

# COMMAND ----------

# DBTITLE 1,Read
df=spark.read.csv(f"{input_path}races.csv",header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,Transform
df1= df.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("path",input_file_name())\
.drop("url")

# COMMAND ----------

# DBTITLE 1,Write
df1.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,count(*) as count from datamaster.formula1.races group by name order by count desc

# COMMAND ----------


