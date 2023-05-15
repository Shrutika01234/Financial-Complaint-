# Databricks notebook source
df=spark.read.parquet("/financial-Complaint-/output.parquet")
df.display(10)

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.select('Complaint ID')

# COMMAND ----------

for column in df.columns:
    print(column)

# COMMAND ----------

column_mapping = {
    "Date received": "date_received",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Consumer complaint narrative": "complaint_what_happened",
    "Company public response": "company_public_response",
    "Company": "company",
    "State": "state",
    "ZIP code": "zip_code",
    "Tags": "tags",
    "Consumer consent provided?": "consumer_consent_provided",
    "Submitted via": "submitted_via",
    "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response",
    "Timely response?": "timely",
    "Consumer disputed?": "consumer_disputed",
    "Complaint ID": "complaint_id",
    
}


for old_name, new_name in column_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)


# COMMAND ----------

for column in df.columns:
    print(column)

# COMMAND ----------

def update_column_attribute(df):
    for column in df.columns:
        setattr(df,column,column)

# COMMAND ----------

update_column_attribute(df)

# COMMAND ----------

## Printing unique values in each column

for column in df.columns:
    print(f"{column}:{df.select(column).distinct().count()}")

# COMMAND ----------

complaint_table="complaint"
df.createOrReplaceTempView(complaint_table)

# COMMAND ----------

n_row = df.count()

# COMMAND ----------

df.groupBy(df.consumer_disputed).count().collect()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

missing_target_df = sql(f"select * from {complaint_table} where {df.consumer_disputed} ='N/A' ")

# COMMAND ----------

df = sql(f"select * from {complaint_table} where {df.consumer_disputed} <>'N/A' ")

# COMMAND ----------


