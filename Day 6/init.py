# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import *
users_schema=StructType([StructField("Id", IntegerType()),
                         StructField("Name", StringType()),
                         StructField("Gender", StringType()),
                         StructField("Salary", IntegerType()),
                         StructField("Country", StringType()),
                         StructField("Date", StringType())
])

# COMMAND ----------

(
spark
.readStream
.schema(users_schema)
.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Ankush/stream")
.trigger(once=True)
.table("asdatabricks.bronze.stream")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from asdatabricks.bronze.stream where Id= 4

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/hexawaredatabricks/raw/stream_in/

# COMMAND ----------

df0=spark.read.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/Feb.CSV",header=True)

# COMMAND ----------

df0.display()

# COMMAND ----------

(
spark
.readStream
.schema(users_schema)
.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/Ankush/stream")
.trigger(once=True)
.table("asdatabricks.bronze.stream")
)
