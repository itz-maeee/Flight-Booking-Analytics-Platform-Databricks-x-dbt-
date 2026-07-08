# Databricks notebook source
src_array = [
    {"src": "bookings"},
    {"src": "flights"},
    {"src": "customers"},
    {"src": "airports"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key",value=src_array)