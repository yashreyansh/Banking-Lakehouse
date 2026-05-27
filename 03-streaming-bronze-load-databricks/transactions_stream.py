# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

# ── Config ────────────────────────────────────────────────
EH_NAMESPACE   = "banking-lakehouse-eventhub-transactions"
EH_TXN_NAME    = "transactioneventhub"
EH_TXN_CONN    = dbutils.secrets.get(
                     scope="banking-lakehouse",
                     key="eventhub_key"
                 )

# DBFS checkpoint — no ADLS needed, built into Databricks

#TXN_CHECKPOINT = "dbfs:/checkpoints/bronze/transactions"
#TXN_TABLE      = "banking_lakehouse.banking_bronze.transactions"   # some issue with UC storage access

#TXN_CHECKPOINT = "/Volumes/banking_lakehouse/banking_bronze/checkpoints/transactions"
#TXN_PATH       = "/Volumes/banking_lakehouse/banking_bronze/raw/transactions"

#TXN_CHECKPOINT = "file:/tmp/checkpoints/bronze/transactions"
#TXN_PATH       = "file:/tmp/bronze/transactions"


TXN_CHECKPOINT = "abfss://unitycatalog@datastoragebanking.dfs.core.windows.net/checkpoints/transactions"
TXN_PATH       = "abfss://unitycatalog@datastoragebanking.dfs.core.windows.net/bronze/transactions"

# ── Kafka / Event Hub Config ──────────────────────────────
bootstrap = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
sasl = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
    f' required username="$ConnectionString" password="{EH_TXN_CONN}";'
)
kafka_options = {
    "kafka.bootstrap.servers":  bootstrap,
    "subscribe":                EH_TXN_NAME,
    "kafka.security.protocol":  "SASL_SSL",
    "kafka.sasl.mechanism":     "PLAIN",
    "kafka.sasl.jaas.config":   sasl,
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "failOnDataLoss":           "false",
    "startingOffsets":          "latest",
}

# ── Schema ────────────────────────────────────────────────
TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType(),    True),
    StructField("account_id",     StringType(),    True),
    StructField("amount",         DoubleType(),    True),
    StructField("currency",       StringType(),    True),
    StructField("txn_type",       StringType(),    True),
    StructField("channel",        StringType(),    True),
    StructField("status",         StringType(),    True),
    StructField("timestamp",      TimestampType(), True),
    StructField("_ingest_time",   TimestampType(), True),
    StructField("_source",        StringType(),    True),
])

# ── Create catalog + schema if not exists ─────────────────
spark.sql("CREATE CATALOG  IF NOT EXISTS banking_lakehouse")
spark.sql("CREATE DATABASE IF NOT EXISTS banking_lakehouse.banking_bronze")

# ── Read Stream from Event Hub ────────────────────────────
print("Connecting to Event Hub...")
raw = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# ── Parse JSON payload ────────────────────────────────────
parsed = (
    raw.select(
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(
            F.col("value").cast("string"),
            TXN_SCHEMA
        ).alias("data")
    )
    .select(
        "data.*",
        "partition", "offset", "kafka_timestamp",
        F.current_timestamp().alias("_bronze_ingest_time"),
        F.lit("event_hub_kafka").alias("_pipeline_source"),
    )
    .filter(F.col("transaction_id").isNotNull())
)

# ── Write to Bronze Delta Table ───────────────────────────
# ── print(f"Starting stream → {TXN_TABLE}")
print(f"Starting stream → {TXN_PATH}")
print(f"Checkpoint    → {TXN_CHECKPOINT}")

query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", TXN_CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(processingTime="30 seconds")   # serverless doesn't support
    #.trigger(availableNow=True)   
    #.toTable(TXN_TABLE)   # need to write to path , as having some UC storage issue
    .start(TXN_PATH) 
)

#print(f"✅ Stream running → {TXN_TABLE}")
print(f"✅ Stream running → {TXN_PATH}")
query.awaitTermination()