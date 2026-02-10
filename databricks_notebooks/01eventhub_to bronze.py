# Databricks notebook source
dbutils.secrets.listScopes()


# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "namespace-hospital-analytics.servicebus.windows.net"
event_hub_name="hospital-analytics-eh"  
event_hub_conn_str = dbutils.secrets.get(scope = "hospitalanalyticsvaultscope", key = "eventhub-connection1")


# hospitalanalyticsvaultscope

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
spark.conf.set("fs.azure.account.key.storageaccfintrack.dfs.core.windows.net",
   dbutils.secrets.get(scope = "hospitalanalyticsvaultscope", key = "storage-connection")
)

bronze_path = "abfss://bronzehsp@storageaccfintrack.dfs.core.windows.net/patient_flow"

#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://bronzehsp@storageaccfintrack.dfs.core.windows.net/_checkpoints/patient_flow")
    .start(bronze_path)
)

# COMMAND ----------

df = spark.read.format("delta").load("abfss://bronzehsp@storageaccfintrack.dfs.core.windows.net/patient_flow")
display(df)

# COMMAND ----------

# 1️ Raw JSON event
manual_json = """
{
  "patient_id": "f648504b-2047-4419-b49b-866600ad0380",
  "gender": "Female",
  "age": 87,
  "department": "Cardiology",
  "admission_time": "2026-02-05T19:56:03.123148",
  "discharge_time": "2026-02-07T20:56:03.123148",
  "bed_id": 63,
  "hospital_id": 5
}
"""

# 2️ Create DataFrame with raw_json column
manual_df = spark.createDataFrame(
    [(manual_json,)],
    ["raw_json"]
)

# 3️ Append into Bronze Delta table
manual_df.write \
    .format("delta") \
    .mode("append") \
    .save(bronze_path)
