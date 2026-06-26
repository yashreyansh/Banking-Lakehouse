import dlt
from pyspark.sql import functions as F

# simple table for now

@dlt.table(
    name="siler_transactions",
    comment = "silver load from bronze transaction delta table"
)
def silver_trans():
    return (
        dlt.readStream("bronze_transactions_stream")
        .filter(F.col("amount")>0)
        .withColumn("processed_at", F.current_timestamp())
        .dropDuplicates(["transaction_id"])
    )