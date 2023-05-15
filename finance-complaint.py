# Databricks notebook source
storage_account_name = "shrutika"
storage_account_access_key = "9XNaCsYXHDwKCH1FCEfWoLEJBJL5kL2vEBVz2F3aEFoGPZ9nHHkGN6mgdbxRAheRJXyhkTa0SMI6+AStLPhRLQ=="

file_location = "wasbs://translateddocs@shrutika.blob.core.windows.net/complaints-2023-05-12_02_12.csv"
file_type = "csv"

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)

from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName('finance_complaint').config('spark.executor.memory', '6g').getOrCreate()


df = spark.read.csv("wasbs://translateddocs@shrutika.blob.core.windows.net/complaints-2023-05-12_02_12.csv", header=True, inferSchema=True)
df
display(df)

df.show()

df.printSchema()

df.describe().show()

df.count()



# COMMAND ----------

df.write.parquet("/financial-Complaint-/output.parquet")


# COMMAND ----------

df=spark.read.parquet("/financial-Complaint-/output.parquet")

# COMMAND ----------

df.display(10)

# COMMAND ----------

df.count()

# COMMAND ----------

df.printSchema()

# COMMAND ----------


