# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG default_location;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE default_location.default.starbucks_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS STARBUCKS
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = 'gs://shibuya_sample/data.csv',
# MAGIC   header = 'true'
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default_location.default.starbucks

# COMMAND ----------

df = spark.read.option("sep", "").option("charset", "shift-jis").options(header="true").csv("gs://shibuya_sample/data.csv")


# COMMAND ----------

display(df)

# COMMAND ----------


