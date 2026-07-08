import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="stage_bookings"
)
def stage_bookings():

    return (
        spark.readStream
        .format("delta")
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
    )


@dlt.view(
    name="trans_bookings"
)
def trans_bookings():

    return (
        dlt.read_stream("stage_bookings")
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("booking_date", to_date(col("booking_date")))
        .withColumn("modifiedDate", current_timestamp())
        .drop("_rescued_data")
    )


rules = {
    "booking_id_not_null": "booking_id IS NOT NULL",
    "passenger_id_not_null": "passenger_id IS NOT NULL"
}


@dlt.table(
    name="silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():

    return dlt.read_stream("trans_bookings")







###############################################
#flight data

@dlt.view(
    name = "trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
              .withColumn("modifiedDate", current_timestamp())
              .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("flight_id"),
  stored_as_scd_type = 1
)



###############################################
#passengers data
@dlt.view(
    name = "trans_passengers"
)
def trans_flights():
    df = spark.readStream.format("delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
              .withColumn("modifiedDate", current_timestamp())
              .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
  target = "silver_passengers",
  source = "trans_passengers",
  keys = ["passenger_id"],
  sequence_by = col("passenger_id"),
  stored_as_scd_type = 1
)


###############################################
#airport data
@dlt.view(
    name = "trans_airports"
)
def trans_flights():
    df = spark.readStream.format("delta")\
              .load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
              .withColumn("modifiedDate", current_timestamp())
              .drop("_rescued_data")

    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
  target = "silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = col("modifiedDate"),
  stored_as_scd_type = 1
)

###############################################
#silver business view

@dlt.table(
    name = "silver_business"
)
def silver_business():
    
    df = dlt.readStream("silver_bookings")\
            .join(dlt.readStream("silver_flights"), ["flight_id"])\
            .join(dlt.readStream("silver_passengers"), ["passenger_id"])\
            .join(dlt.readStream("silver_airports"),["airport_id"])\
            .drop("modifiedDate")
        
    return df

#EXTRAA (Just For Fun)
@dlt.table(
    name = 'silver_business_mat'
)
def silver_business_mat():
    df = dlt.read("silver_business")
    return df
        













