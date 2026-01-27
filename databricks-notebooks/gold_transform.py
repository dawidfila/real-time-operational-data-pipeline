from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr, current_timestamp, to_timestamp, sha2, concat_ws, coalesce
from delta.tables import DeltaTable
from pyspark.sql import Window

storage_account = "<<Storage_Account_Name>>"

# ADLS configuration 
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    "<<Storage_Account_access_key>>"
)

# Paths
silver_path = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<silver_path>>"
gold_dim_process = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<gold_dim_process>>"
gold_dim_region = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<gold_dim_region>>"
gold_dim_source = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<gold_dim_source>>"
gold_fact_events = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<gold_fact_events>>"

# Read silver data
silver_df = spark.read.format("delta").load(silver_path)

# DIMENSION TABLE 1: Process Dimension (SCD Type 2)
incoming_process = (silver_df
                    .select("process_id", "process_stage", "priority")
                    .dropDuplicates(["process_id"])
                    .withColumn("effective_from", current_timestamp())
                   )

# Create target if not exists
if not DeltaTable.isDeltaTable(spark, gold_dim_process):
    incoming_process.withColumn("surrogate_key", F.expr("uuid()")) \
                    .withColumn("effective_to", lit(None).cast("timestamp")) \
                    .withColumn("is_current", lit(True)) \
                    .write.format("delta").mode("overwrite").save(gold_dim_process)

# Load target as DeltaTable
target_process = DeltaTable.forPath(spark, gold_dim_process)

# Create hash to detect changes
incoming_process = incoming_process.withColumn(
    "_hash",
    F.sha2(F.concat_ws("||", 
                       F.coalesce(col("process_stage"), lit("NA")), 
                       F.coalesce(col("priority"), lit("NA"))), 256)
)

# Bring target current hash
target_process_df = spark.read.format("delta").load(gold_dim_process).withColumn(
    "_target_hash",
    F.sha2(F.concat_ws("||", 
                       F.coalesce(col("process_stage"), lit("NA")), 
                       F.coalesce(col("priority"), lit("NA"))), 256)
).select("surrogate_key", "process_id", "process_stage", "priority", "is_current", "_target_hash", "effective_from", "effective_to")

# Create temp views for merge
incoming_process.createOrReplaceTempView("incoming_process_tmp")
target_process_df.createOrReplaceTempView("target_process_tmp")

# Mark old current rows as not current where changed
changes_df = spark.sql("""
SELECT t.surrogate_key, t.process_id
FROM target_process_tmp t
JOIN incoming_process_tmp i
  ON t.process_id = i.process_id
WHERE t.is_current = true AND t._target_hash <> i._hash
""")

changed_keys = [row['surrogate_key'] for row in changes_df.collect()]

if changed_keys:
    keys_str = ",".join([f"'{k}'" for k in changed_keys])
    target_process.update(
        condition = expr(f"is_current = true AND surrogate_key IN ({keys_str})"),
        set = {
            "is_current": expr("false"),
            "effective_to": expr("current_timestamp()")
        }
    )

# Insert new rows for changed & new records
inserts_df = spark.sql("""
SELECT i.process_id, i.process_stage, i.priority, i.effective_from, i._hash
FROM incoming_process_tmp i
LEFT JOIN target_process_tmp t
  ON i.process_id = t.process_id AND t.is_current = true
WHERE t.process_id IS NULL OR t._target_hash <> i._hash
""").withColumn("surrogate_key", F.expr("uuid()")) \
  .withColumn("effective_to", lit(None).cast("timestamp")) \
  .withColumn("is_current", lit(True)) \
  .select("surrogate_key", "process_id", "process_stage", "priority", "effective_from", "effective_to", "is_current")

if inserts_df.count() > 0:
    inserts_df.write.format("delta").mode("append").save(gold_dim_process)

# DIMENSION TABLE 2: Region Dimension (Simple)
incoming_region = (silver_df
                   .select("region")
                   .dropDuplicates(["region"])
                  )

# Use merge to avoid duplicates on incremental loads
if not DeltaTable.isDeltaTable(spark, gold_dim_region):
    incoming_region.withColumn("surrogate_key", F.expr("uuid()")) \
        .select("surrogate_key", "region") \
        .write.format("delta").mode("overwrite").save(gold_dim_region)
else:
    target_region = DeltaTable.forPath(spark, gold_dim_region)
    incoming_region_sk = incoming_region.withColumn("surrogate_key", F.expr("uuid()"))
    
    target_region.alias("t").merge(
        incoming_region_sk.alias("s"),
        "t.region = s.region"
    ).whenNotMatchedInsertAll().execute()

# DIMENSION TABLE 3: Source System Dimension (Simple)
incoming_source = (silver_df
                   .select("source_system")
                   .dropDuplicates(["source_system"])
                  )

# Use merge to avoid duplicates on incremental loads
if not DeltaTable.isDeltaTable(spark, gold_dim_source):
    incoming_source.withColumn("surrogate_key", F.expr("uuid()")) \
        .select("surrogate_key", "source_system") \
        .write.format("delta").mode("overwrite").save(gold_dim_source)
else:
    target_source = DeltaTable.forPath(spark, gold_dim_source)
    incoming_source_sk = incoming_source.withColumn("surrogate_key", F.expr("uuid()"))
    
    target_source.alias("t").merge(
        incoming_source_sk.alias("s"),
        "t.source_system = s.source_system"
    ).whenNotMatchedInsertAll().execute()

# FACT TABLE: Operational Events

# Read current dimensions
dim_process_df = (spark.read.format("delta").load(gold_dim_process)
                  .filter(col("is_current") == True)
                  .select(col("surrogate_key").alias("process_sk"), "process_id"))

dim_region_df = (spark.read.format("delta").load(gold_dim_region)
                 .select(col("surrogate_key").alias("region_sk"), "region"))

dim_source_df = (spark.read.format("delta").load(gold_dim_source)
                 .select(col("surrogate_key").alias("source_sk"), "source_system"))

# Build base fact from silver events
fact_base = (silver_df
             .select("event_id", "process_id", "region", "source_system", 
                     "event_status", "event_start_time", "event_end_time", 
                     "processing_time_sec")
             .withColumn("event_date", F.to_date("event_start_time"))
            )

# Join to get surrogate keys
fact_enriched = (fact_base
                 .join(dim_process_df, on="process_id", how="left")
                 .join(dim_region_df, on="region", how="left")
                 .join(dim_source_df, on="source_system", how="left")
                )

# Create binary flags as integers for Power BI compatibility
fact_enriched = fact_enriched.withColumn(
    "is_success", 
    F.when(col("event_status") == "Completed", 1).otherwise(0)
).withColumn(
    "is_long_running",
    F.when(col("processing_time_sec") > 300, 1).otherwise(0)
).withColumn(
    "event_ingestion_time", 
    current_timestamp()
)

# Final fact table
fact_final = fact_enriched.select(
    col("event_id").alias("fact_id"),
    col("process_sk"),
    col("region_sk"),
    col("source_sk"),
    "event_status",
    "event_start_time",
    "event_end_time",
    "event_date",
    "processing_time_sec",
    "is_success",
    "is_long_running",
    "event_ingestion_time"
)

# Use merge to handle incremental fact loads
if not DeltaTable.isDeltaTable(spark, gold_fact_events):
    fact_final.write.format("delta").mode("overwrite") \
        .partitionBy("event_date") \
        .save(gold_fact_events)
else:
    target_fact = DeltaTable.forPath(spark, gold_fact_events)
    
    target_fact.alias("t").merge(
        fact_final.alias("s"),
        "t.fact_id = s.fact_id"
    ).whenNotMatchedInsertAll().execute()

# Sanity checks
print("Process dim count:", spark.read.format("delta").load(gold_dim_process).count())
print("Region dim count:", spark.read.format("delta").load(gold_dim_region).count())
print("Source System dim count:", spark.read.format("delta").load(gold_dim_source).count())
print("Fact events count:", spark.read.format("delta").load(gold_fact_events).count())