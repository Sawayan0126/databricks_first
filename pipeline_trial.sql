-- Databricks notebook source
CREATE LIVE TABLE tokyo_starbucks
AS SELECT *
FROM default_location.default.external_starbucks
WHERE address like "東京%"

-- COMMAND ----------

CREATE LIVE TABLE tokyo_starbucks2
AS SELECT address, fX, fY
FROM LIVE.tokyo_starbucks

-- COMMAND ----------


