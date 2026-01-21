from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "<<Namespace_hostname>>"
event_hub_name="<<Eventhub_Name>>"  
event_hub_conn_str = "<<Connection_string>>"

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

storage_account = "<<Storage_Account_Name>>"

#ADLS configuration 
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    "<<Storage_Account_access_key>>"
)

bronze_path = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<path>>"
checkpoint_path = f"abfss://<<container>>@{storage_account}.dfs.core.windows.net/<<path>>"

#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path) 
    .start(bronze_path)
)