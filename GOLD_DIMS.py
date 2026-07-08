# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_passengers

# COMMAND ----------

# MAGIC %md
# MAGIC ### **PARAMETERS**

# COMMAND ----------

# #catalog name
# catalog = "workspace"

# #key col list
# key_cols = "['flight_id'] "
# key_col_list = eval(key_cols)

# #cdc col
# cdc_col = "modifiedDate"

# #back dated refresh
# backdated_refresh = ""

# #source object
# source_object = "silver_flights"

# #source schema
# source_schema = "silver"

# #target schema
# target_schema = "gold"

# #target object
# target_object = "DimFlights"

# #surrogate key
# surrogate_key = "DimFlightsKey"

# COMMAND ----------

# #catalog name
# catalog = "workspace"

# #key col list
# key_cols = "['airport_id'] "
# key_col_list = eval(key_cols)

# #cdc col
# cdc_col = "modifiedDate"

# #back dated refresh
# backdated_refresh = ""

# #source object
# source_object = "silver_airports"

# #source schema
# source_schema = "silver"

# #target schema
# target_schema = "gold"

# #target object
# target_object = "DimAirports"

# #surrogate key
# surrogate_key = "DimAirportsKey"

# COMMAND ----------

#catalog name
catalog = "workspace"

#key col list
key_cols = "['passenger_id'] "
key_col_list = eval(key_cols)

#cdc col
cdc_col = "modifiedDate"

#back dated refresh
backdated_refresh = ""

#source object
source_object = "silver_passengers"

#source schema
source_schema = "silver"

#target schema
target_schema = "gold"

#target object
target_object = "DimPassengers"

#surrogate key
surrogate_key = "DimPassengersKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **LAST LOAD DATE**

# COMMAND ----------

#No back dated refresh
if len(backdated_refresh) == 0:

    #if table exist in the destination
    if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

        last_load = spark.sql(f"select max({cdc_col}) from workspace.{target_schema}.{target_object}").collect()[0][0]

    else:
        last_load = "1900-01-01 00:00:00"

#yes back data refresh
else:
    last_load = backdated_refresh

#test the last load
last_load

# COMMAND ----------

df_src = spark.sql(f"select * from {source_schema}.{source_object} where {cdc_col}>'{last_load}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **OLD VS NEW RECORDS**

# COMMAND ----------

#key column String
key_col_string = ', '.join(key_col_list)


# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    #key column String for Incremental
    key_col_string_incremental = ', '.join(key_col_list)

    df_trg = spark.sql(f"SELECT {key_col_string_incremental}, {surrogate_key}, create_date, update_date FROM {catalog}.{target_schema}.{target_object}")

else:
    #key column string for Initial
    key_cols_string_init = [f"'' AS {i}" for i in key_col_list]
    key_cols_string_init = ','.join(key_cols_string_init)
    
    df_trg = spark.sql(f"""SELECT {key_cols_string_init}, CAST('0' as BIGINT) as {surrogate_key}, CAST('1900-01-01 00:00:00' as timestamp) as create_date, CAST('1900-01-01 00:00:00' as timestamp) as update_date where 1=0""")

# COMMAND ----------

df_trg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **JOIN CONDITION**

# COMMAND ----------

join_condition = ' and '.join([f"src.{i} = trg.{i}" for i in key_col_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
          SELECT src.*,
                 trg.{surrogate_key},
                 trg.create_date,
                 trg.update_date
                 FROM src
                 LEFT JOIN trg
                 ON {join_condition}
                 """)

# COMMAND ----------

df_join.display()

# COMMAND ----------

#old records
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())
#new records
df_new = df_join.filter(col(f'{surrogate_key}').isNull())


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **ENRICHING DFS**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Preparing DF_OLD**

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Preparing DF_NEW**

# COMMAND ----------

df_new.display()

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"""
                        select max({surrogate_key}) from {catalog}.{target_schema}.{target_object}
                        """).collect()[0][0]
    
    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())

else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                    .withColumn('create_date', current_timestamp())\
                    .withColumn('update_date', current_timestamp())

# COMMAND ----------

df_new_enr.display()

# COMMAND ----------

df_old_enr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **UNION OLD AND NEW RECORDS**

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):

    dlt_object = DeltaTable.forName(spark,f"{catalog}.{target_schema}.{target_object}")
    dlt_object.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
    .whenMatchedUpdateAll(condition= f"src.{cdc_col} >= trg.{cdc_col}")\
    .whenNotMatchedInsertAll()\
    .execute()


else:
    df_union.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimpassengers