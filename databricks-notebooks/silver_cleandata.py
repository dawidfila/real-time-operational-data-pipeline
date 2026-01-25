from pyspark.sql.types import *
from pyspark.sql.functions import *

storage_account = "<<Storage_Account_Name>>"

# ADLS configuration 
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    "<<Storage_Account_access_key>>"
)

bronze_path = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<bronze_path>>"
silver_path = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<silver_path>>"

# Read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# Define Schema
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("process_id", StringType(), True),
    StructField("process_stage", StringType(), True),
    StructField("event_status", StringType(), True),
    StructField("priority", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("region", StringType(), True),
    StructField("event_start_time", StringType(), True),
    StructField("event_end_time", StringType(), True),
    StructField("processing_time_sec", IntegerType(), True)
])

# Parse JSON to dataframe
parsed_df = bronze_df.withColumn("data", from_json(col("raw_json"), schema)).select("data.*")

# Convert to Timestamp
clean_df = parsed_df \
    .withColumn("event_start_time", to_timestamp("event_start_time")) \
    .withColumn("event_end_time", to_timestamp("event_end_time"))

# Handle invalid start times (NULL or future)
clean_df = clean_df.withColumn(
    "event_start_time",
    when(
        col("event_start_time").isNull() | (col("event_start_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("event_start_time"))
)

# Handle invalid end times (NULL or future)
clean_df = clean_df.withColumn(
    "event_end_time",
    when(
        col("event_end_time").isNull() | (col("event_end_time") > current_timestamp()),
        current_timestamp()
    ).otherwise(col("event_end_time"))
)

# Fix end_time < start_time
clean_df = clean_df.withColumn(
    "event_end_time",
    when(
        col("event_end_time") < col("event_start_time"),
        col("event_start_time")
    ).otherwise(col("event_end_time"))
)

# Fix negative or NULL processing_time_sec
clean_df = clean_df.withColumn(
    "processing_time_sec",
    when(
        col("processing_time_sec").isNull() | (col("processing_time_sec") < 0),
        (unix_timestamp("event_end_time") - unix_timestamp("event_start_time")).cast("int")
    ).otherwise(col("processing_time_sec"))
)

# Schema evolution - ensure expected columns exist
expected_cols = [
    "event_id", "process_id", "process_stage", "event_status",
    "priority", "source_system", "region", "event_start_time",
    "event_end_time", "processing_time_sec"
]

for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None))

# Write to silver table
(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", silver_path + "_checkpoint")
    .start(silver_path)
)