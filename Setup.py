# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME workspace.flight.rawvolume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA workspace.gold;

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/flight/rawvolume/rawdata/airports')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/bronze/bronzevolume/flights/data`