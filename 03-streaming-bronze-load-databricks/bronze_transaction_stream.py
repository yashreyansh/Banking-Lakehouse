# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)
# ── Config ────────────────────────────────────────────────
EH_NAMESPACE   = "banking-events"
EH_TXN_NAME    = "transactioneventhub"
EH_TXN_CONN    = dbutils.secrets.get(
                     scope="banking-lakehouse",
                     key="eventhub_key"
                 )
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
#--------------------------------

@dlt.table(
    name = "bronze_transactions_stream",
    comment = "Transaction from kafka",
    table_properties = {
        "quality":"bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect("valid_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")

def random_table_name():
    return (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
        .select(
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
        
    )

