import dlt
from pyspark.sql import functions as F


EH_NAMESPACE = "banking-lakehouse-eventhub-transactions"
EH_TXN_NAME = "transactioneventhub"

eventhub_key = ""# will add secrests in scope

connection_str = dbutils.secrets.get(scope="banking_lakehouse_secret_scope", key=eventhub_key)

CHECKPOINT_BASE = "abfss://default-container@data1test1sa.dfs.core.windows.net/checkpoint"
TXN_CHECKPOINT   = f"{CHECKPOINT_BASE}/transactions"

 
def eh_kafka_config(conn_string: str, eh_name: str) -> dict:
    """
    Build Kafka options for Azure Event Hub.
    Event Hub exposes a Kafka endpoint on port 9093.
    """
    bootstrap = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
 
    # SASL string — Event Hub uses connection string as the password
    sasl = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        f' required username="$ConnectionString" password="{conn_string}";'
    )
 
    return {
        "kafka.bootstrap.servers":                    bootstrap,
        "subscribe":                                  eh_name,
        "kafka.security.protocol":                    "SASL_SSL",
        "kafka.sasl.mechanism":                       "PLAIN",
        "kafka.sasl.jaas.config":                     sasl,
        "kafka.request.timeout.ms":                   "60000",
        "kafka.session.timeout.ms":                   "30000",
        "failOnDataLoss":                             "false",   # don't crash on expired offsets
        "startingOffsets":                            "latest",  # live mode; use "earliest" for backfill
    }
 
 #---------------------------------------------------------------------------------------------------------------------


TXN_SCHEMA = StructType([
    StructField("transaction_id",      StringType(),    True),
    StructField("account_id",          StringType(),    True),
    StructField("amount",              DoubleType(),    True),
    StructField("currency",            StringType(),    True),
    StructField("txn_type",            StringType(),    True),
    StructField("channel",             StringType(),    True),
    StructField("status",              StringType(),    True),
    StructField("timestamp",            TimestampType(), True),
    StructField("_ingest_time",        TimestampType(), True),
    StructField("_source",             StringType(),    True),
])

#------------------------------------------------------------------------------------------------------------------------

@dlt.table(
    name="transactions",
    comment="Raw transactions coming from event hub (source can be application or any thing else). Adding to bronze - append only, no transformation..."
    table_properties={
        "quality":"bronze",
        "pipeline.reset.allowed":"true",
        "delta.enableChangeDataFeed":"true"
    }
)
@dlt.expect("valid_transaction_id","transaction_id IS NOT NULL")
#@dlt.expect("valid_account_id",     "account_id IS NOT NULL")
#@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0")
@def bronze_transaction():
    kafka_opts = eh_kafka_config(EH_TXN_CONN,EH_TXN_NAME)

    raw = (
        spark.readStream
        .format("kafka")
        .options(**kafka_opts)
        .option("checkpointLocation", EH_TXN_NAME)
        .load()
    )
    # kafka will have topic, partition, offset, timestamp , then json (which wold have our data)
    return (
        raw
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast("string"),    # as kafka stores the data in binary format
                TXN_SCHEMA
                ).alias("data")
        )
        .select(
            "data.*",
            "topic",
            "partition",
            "offset","kafka_timestamp",
            F.current_timestamp().alias("_dlt_ingest_time"),
            F.lit(EH_TXN_NAME).alias("_pipeline_source")
        )
    )




















