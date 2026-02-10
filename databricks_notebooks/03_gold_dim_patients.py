# Databricks notebook source
# MAGIC %md
# MAGIC ### **One-Time Gold Table Creation**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hospital.gold.dim_patient (
# MAGIC   patient_sk STRING,
# MAGIC   patient_id STRING,
# MAGIC   gender STRING,
# MAGIC   age INT,
# MAGIC   attr_hash STRING,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   effective_to TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://goldhsp@storageaccfintrack.dfs.core.windows.net/dim_patient';
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ADLS CONFIG

spark.conf.set(
    "fs.azure.account.key.storageaccfintrack.dfs.core.windows.net",
    dbutils.secrets.get(
        scope="hospitalanalyticsvaultscope",
        key="storage-connection"
    )
)


# PATHS

silver_path = "abfss://silverhsp@storageaccfintrack.dfs.core.windows.net/patient_flow"
gold_dim_patient = "abfss://goldhsp@storageaccfintrack.dfs.core.windows.net/dim_patient"
checkpoint_path = "abfss://control@storageaccfintrack.dfs.core.windows.net/checkpoints/dim_patient"

# READ STREAM

silver_stream = (
    spark.readStream
         .format("delta")
         .load(silver_path)
)


# FOREACH BATCH

def merge_patient(batch_df, batch_id):

    if batch_df.isEmpty():
        return

   
    # 1️ LATEST RECORD PER PATIENT

    w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())

    incoming_patient = (
        batch_df
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .select("patient_id", "gender", "age")
            .withColumn("effective_from", F.current_timestamp())
            .withColumn(
                "attr_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("gender"), F.lit("NA")),
                        F.coalesce(F.col("age").cast("string"), F.lit("NA"))
                    ),
                    256
                )
            )
            .withColumn("patient_sk", F.sha2(F.col("patient_id"), 256))
    )

    target = DeltaTable.forPath(spark, gold_dim_patient)

    
    # 2️ MERGE #1 — EXPIRE CURRENT RECORDS

    (
        target.alias("t")
        .merge(
            incoming_patient.alias("s"),
            "t.patient_id = s.patient_id AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition="t.attr_hash <> s.attr_hash",
            set={
                "is_current": "false",
                "effective_to": "current_timestamp()"
            }
        )
        .execute()
    )

  
    # 3️ MERGE #2 — INSERT NEW CURRENT RECORDS

    (
        target.alias("t")
        .merge(
            incoming_patient.alias("s"),
            "t.patient_id = s.patient_id AND t.is_current = true"
        )
        .whenNotMatchedInsert(
            values={
                "patient_sk": "s.patient_sk",
                "patient_id": "s.patient_id",
                "gender": "s.gender",
                "age": "s.age",
                "attr_hash": "s.attr_hash",
                "effective_from": "s.effective_from",
                "effective_to": "NULL",
                "is_current": "true"
            }
        )
        .execute()
    )


# START STREAM

(
    silver_stream
        .writeStream
        .foreachBatch(merge_patient)
        .option("checkpointLocation", checkpoint_path)
        .trigger(once=True)   # remove for continuous
        .start()
        .awaitTermination()
)

print("Dim Patient SCD Type 2 TWO-MERGE load completed")


# COMMAND ----------

df = spark.read.format("delta").load(gold_dim_patient) 
display(df) 