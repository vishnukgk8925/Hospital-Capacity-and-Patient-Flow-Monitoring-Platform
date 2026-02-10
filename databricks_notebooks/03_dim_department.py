# Databricks notebook source

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ADLS config
spark.conf.set("fs.azure.account.key.storageaccfintrack.dfs.core.windows.net",
 dbutils.secrets.get(scope = "hospitalanalyticsvaultscope", key = "storage-connection"))

silver_path = "abfss://silverhsp@storageaccfintrack.dfs.core.windows.net/patient_flow"
gold_dim_department = "abfss://goldhsp@storageaccfintrack.dfs.core.windows.net/dim_department"
checkpoint_path = "abfss://control@storageaccfintrack.dfs.core.windows.net/checkpoints/dim_department"

#  READ SILVER AS STREAM 
silver_stream = (
    spark.readStream
         .format("delta")
         .load(silver_path)
)

#  PREPARE INCOMING DIM 
incoming_dept = (
    silver_stream
        .select("department", "hospital_id")
        .dropDuplicates(["department", "hospital_id"])
        .withColumn(
            "department_sk",
            F.sha2(
                F.concat_ws("||", "department", "hospital_id"),
                256
            )
        )
)

# FOREACH BATCH LOGIC (initial + incremental)
def merge_department(batch_df, batch_id):

    
    # INITIAL LOAD
    if not DeltaTable.isDeltaTable(spark, gold_dim_department):

        (
            batch_df
                .write
                .format("delta")
                .mode("overwrite")
                .save(gold_dim_department)
        )

        print("Initial load completed for dim_department")
   # INCREMENTAL LOAD
    else:
        target = DeltaTable.forPath(spark, gold_dim_department)

        (
            target.alias("t")
            .merge(
                batch_df.alias("s"),
                "t.department = s.department AND t.hospital_id = s.hospital_id"
            )
            .whenNotMatchedInsert(
                values={
                    "department_sk": "s.department_sk",
                    "department": "s.department",
                    "hospital_id": "s.hospital_id"
                }
            )
            .execute()
        )

        print("Incremental merge completed for dim_department")

# START STREAM 
(
    incoming_dept
        .writeStream
        .foreachBatch(merge_department)
        .option("checkpointLocation", checkpoint_path)
        .trigger(once=True)
        .start()
        .awaitTermination()
)

print("Dim Department incremental load completed")




# COMMAND ----------

df = spark.read.format("delta").load(gold_dim_department)
display(df)