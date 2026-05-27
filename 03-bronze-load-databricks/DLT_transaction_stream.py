# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

# ── Config ────────────────────────────────────────────────
EH_NAMESPACE   = spark.conf.get("eh_namespace")
EH_TXN_NAME    = spark.conf.get("eh_txn_name")
EH_TXN_CONN    = dbutils.secrets.get(
                    scope="banking-lakehouse",
                    key="eventhub_key"
                 )
#CHECKPOINT     = spark.conf.get("checkpoint_base") + "/transactions"

# ── Native Event Hub connection string format ─────────────
# Append EntityPath to connection string
conn_with_entity = f"{EH_TXN_CONN}"

eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_with_entity),
    "eventhubs.consumerGroup":    "$Default",
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    "eventhubs.maxEventsPerTrigger": 1000,
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

# ── DLT Table ─────────────────────────────────────────────
@dlt.table(
    name="transactions",
    comment="Raw transactions from Azure Event Hub. Bronze — append only.",
    table_properties={
        "quality":                    "bronze",
        "pipelines.reset.allowed":    "true",
        "delta.enableChangeDataFeed": "true",
    }
)
@dlt.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect("valid_account_id",     "account_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0")
def bronze_transactions():
    return (
        spark.readStream
        .format("eventhubs")          # ← native format, no Maven needed
        .options(**eh_conf)
        .load()
        .select(
            F.col("enqueuedTime").alias("kafka_timestamp"),
            F.col("offset"),
            F.col("sequenceNumber").alias("sequence"),
            F.col("publisher"),
            F.col("partitionKey"),
            F.from_json(
                F.col("body").cast("string"),   # ← Event Hub uses "body" not "value"
                TXN_SCHEMA
            ).alias("data")
        )
        .select(
            "data.*",
            "kafka_timestamp", "offset", "sequence",
            F.current_timestamp().alias("_bronze_ingest_time"),
            F.lit("event_hub_dlt").alias("_pipeline_source"),
        )
    )