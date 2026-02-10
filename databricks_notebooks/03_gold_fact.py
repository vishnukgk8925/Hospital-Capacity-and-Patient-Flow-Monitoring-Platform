# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

# ADLS config
spark.conf.set("fs.azure.account.key.storageaccfintrack.dfs.core.windows.net",
 dbutils.secrets.get(scope = "hospitalanalyticsvaultscope", key = "storage-connection"))

#  PATHS
silver_path = "abfss://silverhsp@storageaccfintrack.dfs.core.windows.net/patient_flow"
gold_dim_patient = "abfss://goldhsp@storageaccfintrack.dfs.core.windows.net/dim_patient"
gold_dim_department = "abfss://goldhsp@storageaccfintrack.dfs.core.windows.net/dim_department"
gold_fact = "abfss://goldhsp@storageaccfintrack.dfs.core.windows.net/fact_patient_flow"

checkpoint_path = "abfss://control@storageaccfintrack.dfs.core.windows.net/fact_patient_flow"

#  DIM READ (STATIC) 
dim_patient = (
    spark.read.format("delta").load(gold_dim_patient)
    .filter(col("is_current") == True)
    .select("patient_sk", "patient_id")
)

dim_dept = (
    spark.read.format("delta").load(gold_dim_department)
    .select("department_sk", "department", "hospital_id")
)

#  FOREACH BATCH FUNCTION 
def upsert_fact_patient_flow(microbatch_df, batch_id):

    if microbatch_df.isEmpty():
        return

    #  FACT BUILD 
    fact_base = (
        microbatch_df
        .select(
            "patient_id",
            "department",
            "hospital_id",
            "admission_time",
            "discharge_time",
            "bed_id"
        )
        .withColumn("admission_date", F.to_date("admission_time"))
    )

    fact_enriched = (
        fact_base
        .join(dim_patient, "patient_id", "left")
        .join(dim_dept, ["department", "hospital_id"], "left")
        .withColumn(
            "length_of_stay_hours",
            (F.unix_timestamp("discharge_time") -
             F.unix_timestamp("admission_time")) / 3600
        )
        .withColumn(
            "is_currently_admitted",
            F.when(col("discharge_time") > current_timestamp(), True).otherwise(False)
        )
        .withColumn("event_ingestion_time", current_timestamp())
    )

    fact_final = (
        fact_enriched
        .withColumn(
            "fact_id",
            F.xxhash64(
                "patient_id",
                "department",
                "hospital_id",
                "admission_time"
            )
        )
        .select(
            "fact_id",
            "patient_sk",
            "department_sk",
            "admission_time",
            "discharge_time",
            "admission_date",
            "length_of_stay_hours",
            "is_currently_admitted",
            "bed_id",
            "event_ingestion_time"
        )
    )

    #  MERGE 
    if not DeltaTable.isDeltaTable(spark, gold_fact):
        (
            fact_final
            .write.format("delta")
            .partitionBy("admission_date")
            .save(gold_fact)
        )
    else:
        fact_delta = DeltaTable.forPath(spark, gold_fact)

        (
            fact_delta.alias("t")
            .merge(
                fact_final.alias("s"),
                "t.fact_id = s.fact_id"
            )
            .whenMatchedUpdate(set={
                "discharge_time": "s.discharge_time",
                "length_of_stay_hours": "s.length_of_stay_hours",
                "is_currently_admitted": "s.is_currently_admitted",
                "event_ingestion_time": "s.event_ingestion_time"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )

#  STREAM READ 
silver_stream = (
    spark.readStream
    .format("delta")
    .load(silver_path)
)

#  STREAM WRITE 
(
    silver_stream
    .writeStream
    .foreachBatch(upsert_fact_patient_flow)
    .option("checkpointLocation", checkpoint_path)
    .trigger(once=True)   # rerun-safe incremental
    .start()
)


# COMMAND ----------

df = spark.read.format("delta").load(gold_fact)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists hospital.gold.fact_patient_flow 
# MAGIC using delta
# MAGIC location  "abfss://goldhsp@storageaccfintrack.dfs.core.windows.net/fact_patient_flow"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.gender,
# MAGIC     COUNT(CASE WHEN f.is_currently_admitted THEN f.bed_id END) * 1.0 / COUNT(f.bed_id) * 100 AS bed_occupancy_percent
# MAGIC FROM hospital.gold.fact_patient_flow f
# MAGIC JOIN hospital.gold.dim_patient p ON f.patient_sk = p.patient_sk
# MAGIC GROUP BY p.gender;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     p.gender,
# MAGIC     COUNT(DISTINCT f.fact_id) * 1.0 / COUNT(DISTINCT f.bed_id) AS bed_turnover_rate
# MAGIC FROM hospital.gold.fact_patient_flow f
# MAGIC JOIN hospital.gold.dim_patient p ON f.patient_sk = p.patient_sk
# MAGIC GROUP BY p.gender;