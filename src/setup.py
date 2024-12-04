# Databricks notebook source
dbutils.widgets.text("catalog_use", "main")
dbutils.widgets.text("schema_use", "hm_dday")

# COMMAND ----------

catalog_use = dbutils.widgets.get("catalog_use")
schema_use = dbutils.widgets.get("schema_use")

# COMMAND ----------

spark.sql(f"create catalog if not exists {catalog_use}")
spark.sql(f"use catalog {catalog_use}")
display(
  spark.sql("select current_catalog()")
)

# COMMAND ----------

spark.sql(f"create schema if not exists {schema_use}")
spark.sql(f"use schema {schema_use}")
display(
  spark.sql("select current_catalog(), current_schema()")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS landing;
# MAGIC SHOW VOLUMES;

# COMMAND ----------

from pyspark.sql.functions import col

data = spark.read.text("s3://hls-eng-data-public/data/synthea/fhir/fhir/*json", wholetext=True).select(col("value").alias("resource"))

# COMMAND ----------

import json
import os

output_path = "/Volumes/main/hm_dday/landing/fhir/"

# Ensure the output directory exists
dbutils.fs.mkdirs(output_path)

# Write each row as an individual JSON file
for idx, row in enumerate(data.collect()):
    file_path = os.path.join(output_path, f"resource_{idx}.json")
    dbutils.fs.put(file_path, row['resource'], overwrite=True)
