from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "hosp-analytics-ns.servicebus.windows.net"
event_hub_name="rish-hosp-analytics-eh"  
event_hub_conn_str = dbutils.secrets.get(scope="hosp-vault-scope", key="eventhub-connection")
  
kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.hospdata.dfs.core.windows.net",
   dbutils.secrets.get(scope="hosp-vault-scope", key="storage-connection")
  )



bronze_path = "abfss://bronze@hospdata.dfs.core.windows.net/patient_flow"

#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://bronze@hospdata.dfs.core.windows.net/_checkpoints/patient_flow")
    .start(bronze_path)
    
    
)