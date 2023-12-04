# Databricks notebook source
df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/users")
        


# COMMAND ----------

dbutils.fs.ls("/path/to/directory")


# COMMAND ----------

dbutils.fs.ls("/")


# COMMAND ----------

print(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sample_table (
# MAGIC     id INT,
# MAGIC     name STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sample_table VALUES
# MAGIC     (1, 'Alice'),
# MAGIC     (2, 'Bob'),
# MAGIC     (3, 'Charlie'),
# MAGIC     (4, 'David'),
# MAGIC     (5, 'Eva'),
# MAGIC     (6, 'Frank'),
# MAGIC     (7, 'Grace'),
# MAGIC     (8, 'Hannah'),
# MAGIC     (9, 'Igor'),
# MAGIC     (10, 'Jack')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sample_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended sample_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sawayan.default.first_table (
# MAGIC     id INT,
# MAGIC     name STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended sawayan.default.first_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test_external (
# MAGIC     id INT,
# MAGIC     name STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION 'gs://shibuya_sample/ex'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sawayan.default.second_table VALUES
# MAGIC     (1, 'Alice'),
# MAGIC     (2, 'Bob'),
# MAGIC     (3, 'Charlie'),
# MAGIC     (4, 'David'),
# MAGIC     (5, 'Eva'),
# MAGIC     (6, 'Frank'),
# MAGIC     (7, 'Grace'),
# MAGIC     (8, 'Hannah'),
# MAGIC     (9, 'Igor'),
# MAGIC     (10, 'Jack')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sawayan.default.second_table

# COMMAND ----------

csv_path = 'gs://shibuya_sample/test_directory/starbucks.csv'
df = spark.read.option('header', 'true').csv(csv_path)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

table_name = 'sawayan.default.starbucks'
df.write.format("delta").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sawayan.default.starbucks

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view test_view
# MAGIC   (address STRING,
# MAGIC    LocName STRING,
# MAGIC    fX INT,
# MAGIC    fY INT,
# MAGIC    iConf INT,
# MAGIC    iLvl INT)
# MAGIC using csv
# MAGIC options (
# MAGIC   path = 'gs://shibuya_sample/test_directory/starbucks.csv',
# MAGIC   header = 'true'
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table sawayan.default.external_starbucks
# MAGIC using delta
# MAGIC location 'gs://shibuya_sample/tables/starbucks'
# MAGIC as
# MAGIC select * from test_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   address, 
# MAGIC   LocName
# MAGIC from sawayan.default.external_starbucks
# MAGIC where address like '東京%'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sawayan.default.second_table VALUES
# MAGIC     (1, 'Alice'),
# MAGIC     (2, 'Bob'),
# MAGIC     (3, 'Charlie'),
# MAGIC     (4, 'David'),
# MAGIC     (5, 'Eva'),
# MAGIC     (6, 'Frank'),
# MAGIC     (7, 'Grace'),
# MAGIC     (8, 'Hannah'),
# MAGIC     (9, 'Igor'),
# MAGIC     (10, 'Jack')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sawayan.default.second_table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sawayan.default.second_table

# COMMAND ----------

# MAGIC %sql
# MAGIC restore sawayan.default.second_table to version as of 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sawayan.default.second_table

# COMMAND ----------

df = spark.read.csv("/Workspace/Users/k.sawaya@estyle-inc.jp.estyle3.csv")
df.show()

# COMMAND ----------

import pandas as pd 
df = pd.read_csv("/Workspace/Users/k.sawaya@estyle-inc.jp/data/shibuya_faq.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table default_location.default.test_external
# MAGIC location "gs://shibuya_sample/test"

# COMMAND ----------


