# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path= "dbfs:/mnt/datamasterdatabricks/raw/dataset/formula1/"

# COMMAND ----------

catalog="datamaster";
schema="formula1"
