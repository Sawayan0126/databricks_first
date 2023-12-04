-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##テーブルの作成

-- COMMAND ----------

-- MAGIC %md
-- MAGIC マネージドテーブルの作成

-- COMMAND ----------

--　データベースの指定なしでマネージドテーブルを作成
CREATE TABLE IF NOT EXISTS managed_table

-- COMMAND ----------

--　hive_metastore.default内にテーブルが作られる
-- ロケーションはdbfs:/user/hive/warehouse
DESCRIBE TABLE EXTENDED managed_table

-- COMMAND ----------

-- データベースの指定ありでマネージドテーブルの作成
CREATE TABLE IF NOT EXISTS default_location.default.managed_table

-- COMMAND ----------

-- ワークスペースが割り当てられているメタストアのデフォルトの外部ロケーションに保存される
DESCRIBE TABLE EXTENDED default_location.default.managed_table

-- COMMAND ----------

-- 別のカタログを指定してマネージドテーブルを作成
CREATE TABLE IF NOT EXISTS second_location.default.managed_table

-- COMMAND ----------

--　カタログの外部ロケーションに保存される
DESCRIBE TABLE EXTENDED second_location.default.managed_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 外部テーブルの作成

-- COMMAND ----------

-- データベースの指定なしで外部テーブルを作成
CREATE EXTERNAL TABLE IF NOT EXISTS external_table
LOCATION 'dbfs:/user/hive/practice'

--　クラウドの外部ロケーション指定だと何故か失敗する、、クラスターのせい？
-- CREATE EXTERNAL TABLE IF NOT EXISTS external_table
-- LOCATION 'gs://for_hive_metastore/practice'

-- COMMAND ----------

--　hive_metastore.default内にテーブルが作られる
DESCRIBE TABLE EXTENDED external_table

-- COMMAND ----------

-- データベースの指定ありで外部テーブルを作成
-- メタストアと紐付けたデフォルトの外部ロケーションはロケーション指定できない
CREATE EXTERNAL TABLE IF NOT EXISTS default_location.default.external_table
LOCATION 'gs://shibuya_sample/practice'

-- COMMAND ----------

-- 指定した外部ロケーション内にテーブルが保存される
DESCRIBE TABLE EXTENDED default_location.default.external_table

-- COMMAND ----------

-- 別のカタログを指定して外部テーブルを作成
CREATE EXTERNAL TABLE IF NOT EXISTS second_location.default.external_table
LOCATION 'gs://shibuya_sample/practice1'

-- COMMAND ----------

-- 指定した外部ロケーションにテーブルが作成される
DESCRIBE TABLE EXTENDED second_location.default.external_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - 備考
-- MAGIC   - メタストア作成時にデフォルトの外部ロケーションを設定しなかった場合、マネージドテーブルは全てdbfs:/user/hive/warehouseに保存される
-- MAGIC   - 形式を指定しなかった場合、マネージド、外部問わずdelta形式となる
